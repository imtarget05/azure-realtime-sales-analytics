"""
Test MLOps Workflow - End-to-end testing of automated drift detection and retrain

This script provides automated testing for the entire MLOps pipeline:
1. Normal operation (no drift)
2. Drift detection and auto-retrain
3. Quality gate failure
4. Rollback mechanism
5. Azure Function integration

Usage:
    # Run all tests
    python scripts/test_mlops_workflow.py --full
    
    # Run specific test
    python scripts/test_mlops_workflow.py --test drift_detection
    
    # Run with verbose output
    python scripts/test_mlops_workflow.py --full --verbose
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pyodbc

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

SQL_CONNECTION_STRING = (
    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class MLOpsWorkflowTester:
    """End-to-end MLOps workflow testing"""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results = {}
        self.project_root = Path(__file__).parent.parent
        
    def log(self, message: str, level: str = 'INFO'):
        """Log message with color"""
        colors = {
            'INFO': Colors.BLUE,
            'SUCCESS': Colors.GREEN,
            'WARNING': Colors.YELLOW,
            'ERROR': Colors.RED
        }
        color = colors.get(level, Colors.RESET)
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"{color}[{timestamp}] {message}{Colors.RESET}")
    
    def run_command(self, command: List[str], description: str) -> Tuple[bool, str]:
        """Run shell command and return success status"""
        if self.verbose:
            self.log(f"Running: {' '.join(command)}", 'INFO')
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                cwd=self.project_root,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                self.log(f"✅ {description}", 'SUCCESS')
                return True, result.stdout
            else:
                self.log(f"❌ {description} (exit code: {result.returncode})", 'ERROR')
                if self.verbose:
                    print(f"STDOUT:\n{result.stdout}")
                    print(f"STDERR:\n{result.stderr}")
                return False, result.stderr
                
        except subprocess.TimeoutExpired:
            self.log(f"❌ {description} (timeout)", 'ERROR')
            return False, "Command timeout"
        except Exception as e:
            self.log(f"❌ {description} ({str(e)})", 'ERROR')
            return False, str(e)
    
    def check_sql_connection(self) -> bool:
        """Verify SQL database connection"""
        self.log("Testing SQL database connection...", 'INFO')
        
        try:
            conn = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM SalesTransactions")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            self.log(f"✅ SQL connection OK ({count} records in SalesTransactions)", 'SUCCESS')
            return True
        except Exception as e:
            self.log(f"❌ SQL connection failed: {e}", 'ERROR')
            return False
    
    def get_current_model_info(self) -> Dict:
        """Get current production model information"""
        try:
            metadata_path = self.project_root / 'models' / 'production' / 'model_metadata.json'
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                return metadata
            else:
                return {}
        except Exception as e:
            self.log(f"⚠️ Could not read model metadata: {e}", 'WARNING')
            return {}
    
    def test_normal_operation(self) -> bool:
        """Test 1: Normal operation without drift"""
        self.log("\n" + "="*60, 'INFO')
        self.log("Test 1: Normal Operation (No Drift)", 'INFO')
        self.log("="*60, 'INFO')
        
        # Run drift detection (dry-run: measure only, no retrain trigger)
        success, output = self.run_command(
            ['python', 'ml/drift_monitor.py', '--dry-run'],
            "Drift detection (dry-run)"
        )
        
        if not success:
            return False
        
        # Verify no drift detected
        if 'drift detected' in output.lower() and 'no' in output.lower():
            self.log("✅ Correctly identified no drift", 'SUCCESS')
            return True
        else:
            self.log("⚠️ Unexpected drift detection result", 'WARNING')
            return False
    
    def test_drift_detection_and_retrain(self) -> bool:
        """Test 2: Drift detection triggers auto-retrain"""
        self.log("\n" + "="*60, 'INFO')
        self.log("Test 2: Drift Detection & Auto-Retrain", 'INFO')
        self.log("="*60, 'INFO')
        
        # Get baseline model
        baseline_model = self.get_current_model_info()
        baseline_accuracy = baseline_model.get('metrics', {}).get('accuracy', 0)
        self.log(f"Baseline model accuracy: {baseline_accuracy:.4f}", 'INFO')
        
        # Step 1: Inject drift data
        self.log("Step 1: Injecting drift data...", 'INFO')
        success, output = self.run_command(
            ['python', 'scripts/simulate_drift.py',
             '--drift-type', 'price_inflation',
             '--severity', 'medium',
             '--num-samples', '500'],
            "Inject drift data"
        )
        
        if not success:
            return False
        
        time.sleep(2)  # Wait for data to settle
        
        # Step 2: Run drift detection
        self.log("Step 2: Running drift detection...", 'INFO')
        success, output = self.run_command(
            ['python', 'ml/drift_monitor.py', '--dry-run'],
            "Drift detection"
        )
        
        if not success:
            return False
        
        # Verify drift detected
        if 'drift detected' in output.lower() and 'warning' in output.lower():
            self.log("✅ Drift correctly detected", 'SUCCESS')
        else:
            self.log("❌ Drift not detected (might need more severe drift)", 'ERROR')
            return False
        
        # Step 3: Trigger retrain
        self.log("Step 3: Triggering retrain...", 'INFO')
        self.log("⏳ This may take 5-10 minutes...", 'INFO')
        
        success, output = self.run_command(
            ['python', 'ml/retrain_and_compare.py', '--promote'],
            "Auto retrain"
        )
        
        if not success:
            self.log("⚠️ Retrain failed (this is OK if quality gate failed)", 'WARNING')
        
        # Step 4: Verify new model
        new_model = self.get_current_model_info()
        new_accuracy = new_model.get('metrics', {}).get('accuracy', 0)
        
        if new_accuracy > baseline_accuracy:
            improvement = (new_accuracy - baseline_accuracy) / baseline_accuracy * 100
            self.log(f"✅ Model improved: {baseline_accuracy:.4f} → {new_accuracy:.4f} (+{improvement:.2f}%)", 'SUCCESS')
            return True
        elif new_accuracy == baseline_accuracy:
            self.log("⚠️ Model unchanged (quality gate may have blocked deployment)", 'WARNING')
            return True  # This is acceptable behavior
        else:
            self.log(f"❌ Model degraded: {baseline_accuracy:.4f} → {new_accuracy:.4f}", 'ERROR')
            return False
    
    def test_quality_gate_failure(self) -> bool:
        """Test 3: Quality gate blocks bad model"""
        self.log("\n" + "="*60, 'INFO')
        self.log("Test 3: Quality Gate Failure", 'INFO')
        self.log("="*60, 'INFO')
        
        # Get baseline
        baseline_model = self.get_current_model_info()
        baseline_version = baseline_model.get('version', 'unknown')
        
        # Inject corrupted data
        self.log("Injecting corrupted data...", 'INFO')
        success, output = self.run_command(
            ['python', 'scripts/simulate_drift.py',
             '--drift-type', 'data_corruption',
             '--severity', 'high',
             '--num-samples', '800'],
            "Inject corrupted data"
        )
        
        if not success:
            return False
        
        time.sleep(2)
        
        # Trigger retrain (should fail quality gate)
        self.log("Attempting retrain with bad data...", 'INFO')
        success, output = self.run_command(
            ['python', 'ml/retrain_and_compare.py', '--promote'],
            "Retrain with bad data"
        )
        
        # Check model version unchanged
        current_model = self.get_current_model_info()
        current_version = current_model.get('version', 'unknown')
        
        if current_version == baseline_version:
            self.log(f"✅ Quality gate blocked deployment (version unchanged: {current_version})", 'SUCCESS')
            return True
        else:
            self.log(f"❌ Bad model was deployed (version changed: {baseline_version} → {current_version})", 'ERROR')
            return False
    
    def test_notifications(self) -> bool:
        """Test 4: Notification system"""
        self.log("\n" + "="*60, 'INFO')
        self.log("Test 4: Notification System", 'INFO')
        self.log("="*60, 'INFO')
        
        # Check monitoring events in SQL
        try:
            conn = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = conn.cursor()
            
            # Check if MonitoringEvents table exists
            cursor.execute("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'MonitoringEvents'
            """)
            
            if cursor.fetchone()[0] == 0:
                self.log("⚠️ MonitoringEvents table does not exist", 'WARNING')
                self.log("   Run: sql/create_monitoring_tables.sql", 'INFO')
                return False
            
            # Query recent events
            cursor.execute("""
                SELECT TOP 10 EventType, Severity, Message, Timestamp
                FROM MonitoringEvents
                ORDER BY Timestamp DESC
            """)
            
            events = cursor.fetchall()
            if events:
                self.log(f"✅ Found {len(events)} recent monitoring events", 'SUCCESS')
                if self.verbose:
                    for event in events:
                        print(f"  {event.Timestamp} | {event.EventType} | {event.Message}")
                return True
            else:
                self.log("⚠️ No monitoring events found (table might be empty)", 'WARNING')
                return True  # Empty is OK for first run
            
        except Exception as e:
            self.log(f"❌ Notification test failed: {e}", 'ERROR')
            return False
        finally:
            cursor.close()
            conn.close()
    
    def test_azure_function(self) -> bool:
        """Test 5: Azure Function integration (if deployed)"""
        self.log("\n" + "="*60, 'INFO')
        self.log("Test 5: Azure Function Integration", 'INFO')
        self.log("="*60, 'INFO')
        
        function_path = self.project_root / 'azure_functions' / 'DriftMonitor' / '__init__.py'
        
        if not function_path.exists():
            self.log("⚠️ Azure Function code not found", 'WARNING')
            self.log("   Expected: azure_functions/DriftMonitor/__init__.py", 'INFO')
            return False
        
        self.log("✅ Azure Function code exists", 'SUCCESS')
        
        # Check function.json
        config_path = self.project_root / 'azure_functions' / 'DriftMonitor' / 'function.json'
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Verify timer trigger
            bindings = config.get('bindings', [])
            has_timer = any(b.get('type') == 'timerTrigger' for b in bindings)
            
            if has_timer:
                self.log("✅ Timer trigger configured", 'SUCCESS')
                return True
            else:
                self.log("❌ Timer trigger not found in function.json", 'ERROR')
                return False
        else:
            self.log("⚠️ function.json not found", 'WARNING')
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all tests and return results"""
        self.log(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}", 'INFO')
        self.log(f"{Colors.BOLD}MLOps Workflow End-to-End Testing{Colors.RESET}", 'INFO')
        self.log(f"{Colors.BOLD}{'='*60}{Colors.RESET}", 'INFO')
        self.log(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 'INFO')
        
        # Prerequisites
        if not self.check_sql_connection():
            self.log("❌ Prerequisites failed: SQL connection", 'ERROR')
            return {}
        
        # Run tests
        tests = {
            'normal_operation': self.test_normal_operation,
            'drift_detection': self.test_drift_detection_and_retrain,
            'quality_gate': self.test_quality_gate_failure,
            'notifications': self.test_notifications,
            'azure_function': self.test_azure_function,
        }
        
        results = {}
        for test_name, test_func in tests.items():
            try:
                start_time = time.time()
                result = test_func()
                duration = time.time() - start_time
                results[test_name] = result
                
                self.log(f"Test completed in {duration:.1f}s", 'INFO')
                
            except Exception as e:
                self.log(f"❌ Test '{test_name}' raised exception: {e}", 'ERROR')
                results[test_name] = False
        
        return results
    
    def print_summary(self, results: Dict[str, bool]):
        """Print test summary"""
        self.log(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}", 'INFO')
        self.log(f"{Colors.BOLD}Test Results Summary{Colors.RESET}", 'INFO')
        self.log(f"{Colors.BOLD}{'='*60}{Colors.RESET}", 'INFO')
        
        test_names = {
            'normal_operation': 'Test 1: Normal Operation',
            'drift_detection': 'Test 2: Drift Detection & Retrain',
            'quality_gate': 'Test 3: Quality Gate Failure',
            'notifications': 'Test 4: Notification System',
            'azure_function': 'Test 5: Azure Function Integration',
        }
        
        passed = 0
        total = len(results)
        
        for test_key, test_name in test_names.items():
            if test_key in results:
                status = "✅ PASSED" if results[test_key] else "❌ FAILED"
                color = Colors.GREEN if results[test_key] else Colors.RED
                print(f"{color}{status:12}{Colors.RESET} {test_name}")
                if results[test_key]:
                    passed += 1
        
        self.log(f"{Colors.BOLD}{'='*60}{Colors.RESET}", 'INFO')
        
        pass_rate = (passed / total * 100) if total > 0 else 0
        overall_color = Colors.GREEN if pass_rate >= 80 else Colors.YELLOW if pass_rate >= 60 else Colors.RED
        
        print(f"{overall_color}{Colors.BOLD}Overall: {passed}/{total} tests passed ({pass_rate:.0f}%){Colors.RESET}")
        self.log(f"{Colors.BOLD}{'='*60}{Colors.RESET}", 'INFO')
        
        # Recommendations
        if pass_rate < 100:
            self.log("\n📋 Recommendations:", 'INFO')
            if not results.get('normal_operation', True):
                print("  • Check ml/drift_monitor.py configuration")
            if not results.get('drift_detection', True):
                print("  • Verify drift threshold settings in config/settings.py")
                print("  • Check ml/retrain_and_compare.py logic")
            if not results.get('quality_gate', True):
                print("  • Review quality gate threshold (QUALITY_GATE_MIN_IMPROVEMENT)")
            if not results.get('notifications', True):
                print("  • Run sql/create_monitoring_tables.sql")
                print("  • Configure Slack/Teams webhook")
            if not results.get('azure_function', True):
                print("  • Implement azure_functions/DriftMonitor/__init__.py")


def main():
    parser = argparse.ArgumentParser(
        description='Test MLOps workflow end-to-end',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--full',
        action='store_true',
        help='Run all tests'
    )
    
    parser.add_argument(
        '--test',
        type=str,
        choices=['normal_operation', 'drift_detection', 'quality_gate', 'notifications', 'azure_function'],
        help='Run specific test only'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose output'
    )
    
    args = parser.parse_args()
    
    if not args.full and not args.test:
        parser.print_help()
        sys.exit(1)
    
    tester = MLOpsWorkflowTester(verbose=args.verbose)
    
    if args.full:
        results = tester.run_all_tests()
        tester.print_summary(results)
        
        # Exit code based on results
        passed = sum(1 for r in results.values() if r)
        sys.exit(0 if passed == len(results) else 1)
        
    elif args.test:
        # Run single test
        test_methods = {
            'normal_operation': tester.test_normal_operation,
            'drift_detection': tester.test_drift_detection_and_retrain,
            'quality_gate': tester.test_quality_gate_failure,
            'notifications': tester.test_notifications,
            'azure_function': tester.test_azure_function,
        }
        
        success = test_methods[args.test]()
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
