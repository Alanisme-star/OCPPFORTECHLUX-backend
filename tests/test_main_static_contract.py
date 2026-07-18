import ast
import unittest
from pathlib import Path


MAIN_PATH = Path(__file__).resolve().parents[1] / "main.py"


class MainStaticContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = MAIN_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def _function(self, name):
        return next(
            node
            for node in ast.walk(self.tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == name
        )

    def test_only_one_stop_route_decorator_exists(self):
        count = 0
        for node in ast.walk(self.tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call) or not decorator.args:
                    continue
                value = decorator.args[0]
                if (
                    isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                    and value.value.endswith("/stop")
                    and "charge-points" in value.value
                ):
                    count += 1
        self.assertEqual(count, 1)

    def test_meter_values_does_not_await_auto_stop(self):
        function = self._function("on_meter_values")
        awaited_names = {
            node.value.func.id
            for node in ast.walk(function)
            if isinstance(node, ast.Await)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
        }
        self.assertNotIn("_auto_stop_if_balance_insufficient", awaited_names)

    def test_meter_values_has_no_await_inside_database_with_block(self):
        function = self._function("on_meter_values")
        violations = []
        for node in ast.walk(function):
            if not isinstance(node, ast.With):
                continue
            expression = ast.get_source_segment(
                self.source, node.items[0].context_expr
            ) or ""
            if "get_conn" not in expression and "sqlite3.connect" not in expression:
                continue
            if any(isinstance(child, ast.Await) for child in ast.walk(node)):
                violations.append(node.lineno)
        self.assertEqual(violations, [])

    def test_monitor_calls_service_not_route_handler(self):
        function = self._function("monitor_balance_and_auto_stop")
        call_names = {
            node.func.id
            for node in ast.walk(function)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        self.assertIn("request_transaction_stop", call_names)
        self.assertNotIn("stop_transaction_by_charge_point", call_names)

    def test_pending_stop_subscripts_do_not_use_cp_id_or_str_key(self):
        violations = []
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Subscript):
                continue
            if not (
                isinstance(node.value, ast.Name)
                and node.value.id == "pending_stop_transactions"
            ):
                continue
            key = node.slice
            if isinstance(key, ast.Name) and key.id == "cp_id":
                violations.append(node.lineno)
            if (
                isinstance(key, ast.Call)
                and isinstance(key.func, ast.Name)
                and key.func.id == "str"
            ):
                violations.append(node.lineno)
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
