"""
T25 static half (fastapi-free AST, R3-M4): broadcast_signal validates with signal_guard as its first statement,
before its try block (whose except would turn a 422 into a 500); SignalBroadcast.risk_pct is a required float.
"""
import ast
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _tree():
    with open(os.path.join(ROOT, 'follower_endpoints.py'), encoding='utf-8') as f:
        return ast.parse(f.read())


def _func(tree, name):
    return next(n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name)


def test_signal_guard_validation_is_the_first_statement_before_the_try():
    fn = _func(_tree(), 'broadcast_signal')
    body = fn.body[1:] if isinstance(fn.body[0], ast.Expr) and isinstance(fn.body[0].value, ast.Constant) else fn.body
    first = body[0]
    assert isinstance(first, ast.Assign) and ast.unparse(first.value.func) == 'signal_guard.validate_signal_payload'
    assert [ast.unparse(a) for a in first.value.args] == [
        'signal.action', 'signal.entry_price', 'signal.stop_loss', 'signal.take_profit', 'signal.leverage',
        'signal.risk_pct']
    check = body[1]
    assert isinstance(check, ast.If) and isinstance(check.body[0], ast.Raise)
    assert "status_code=422" in ast.unparse(check.body[0])
    try_index = next(i for i, s in enumerate(body) if isinstance(s, ast.Try))
    assert try_index > 1


def test_risk_pct_is_a_required_float():
    cls = next(n for n in ast.walk(_tree()) if isinstance(n, ast.ClassDef) and n.name == 'SignalBroadcast')
    field = next(s for s in cls.body if isinstance(s, ast.AnnAssign) and s.target.id == 'risk_pct')
    assert ast.unparse(field.annotation) == 'float'
    assert field.value is None


def test_sec2_master_key_has_no_default_and_is_compared_in_constant_time():
    tree = _tree()
    assign = next(n for n in tree.body if isinstance(n, ast.Assign)
                  and any(getattr(t, 'id', None) == 'MASTER_API_KEY' for t in n.targets))
    assert ast.unparse(assign.value) == "os.getenv('MASTER_API_KEY')"
    fn = _func(tree, 'verify_master_key')
    assert 'hmac.compare_digest' in {ast.unparse(c.func) for c in ast.walk(fn) if isinstance(c, ast.Call)}
    assert 'status_code=503' in ast.unparse(fn) and 'status_code=401' in ast.unparse(fn)


# ---------------------------------------------------------------- SEC-3 (QC security-2): the admin password
def _main_tree():
    with open(os.path.join(ROOT, 'main.py'), encoding='utf-8') as f:
        return ast.parse(f.read())


def test_sec3_admin_password_has_no_committed_default():
    with open(os.path.join(ROOT, 'admin_dashboard.py'), encoding='utf-8') as f:
        tree = ast.parse(f.read())
    assign = next(n for n in tree.body if isinstance(n, ast.Assign)
                  and any(getattr(t, 'id', None) == 'ADMIN_PASSWORD' for t in n.targets))
    assert ast.unparse(assign.value) == "os.getenv('ADMIN_PASSWORD')"
    for path in ('main.py', 'admin_dashboard.py'):
        with open(os.path.join(ROOT, path), encoding='utf-8') as f:
            assert 'changeme' not in f.read()


def test_sec3_every_admin_check_goes_through_the_constant_time_helper():
    tree = _main_tree()
    compares = [ast.unparse(n) for n in ast.walk(tree) if isinstance(n, ast.Compare)
                and 'ADMIN_PASSWORD' in ast.unparse(n)]
    assert compares == []                                                   # no plain == / != on the password
    fn = _func(tree, 'admin_password_ok')
    assert 'hmac.compare_digest' in {ast.unparse(c.func) for c in ast.walk(fn) if isinstance(c, ast.Call)}
    assert 'status_code=503' in ast.unparse(fn)
    uses = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and ast.unparse(n.func) == 'admin_password_ok']
    assert len(uses) >= 20


def test_sec3_admin_password_ok_behaviour():
    tree = _main_tree()
    fn = _func(tree, 'admin_password_ok')
    import hmac

    class HTTPException(Exception):
        def __init__(self, status_code, detail=None):
            self.status_code = status_code

    def load(password):
        ns = {'hmac': hmac, 'HTTPException': HTTPException, 'ADMIN_PASSWORD': password}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec'), ns)
        return ns['admin_password_ok']
    for unset in (None, ''):
        try:
            load(unset)('anything')
            raise AssertionError('an unset ADMIN_PASSWORD must answer 503')
        except HTTPException as e:
            assert e.status_code == 503
    ok = load('s3cret-value')
    assert ok('s3cret-value') is True
    assert ok('s3cret-valuX') is False and ok('') is False and ok(None) is False


def test_sec3_the_admin_password_is_never_printed_or_advertised():
    with open(os.path.join(ROOT, 'main.py'), encoding='utf-8') as f:
        src = f.read()
    tree = ast.parse(src)
    prints = [ast.unparse(n) for n in ast.walk(tree) if isinstance(n, ast.Call) and ast.unparse(n.func) == 'print'
              and ('ADMIN_PASSWORD' in ast.unparse(n) or 'x_admin_key' in ast.unparse(n))]
    assert prints == []
    root = _func(tree, 'root')
    assert 'reset-database' not in ast.unparse(root) and 'password=' not in ast.unparse(root)
