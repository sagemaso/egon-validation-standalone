import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

# Mock the external dependencies that might not be available
with patch.dict('sys.modules', {
    'sshtunnel': Mock(),
    'dotenv': Mock(),
    'sqlalchemy': Mock(),
    'sqlalchemy.orm': Mock(),
}):
    from src.rules.formal.null_check_rule import NullCheckRule
    from src.core.validation_result import ValidationResult


class TestNullCheckRule(unittest.TestCase):
    """Test suite for NullCheckRule"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock()
        self.mock_engine = Mock()
        self.null_check_rule = NullCheckRule(db_manager=self.mock_db_manager)

    def test_init(self):
        """Test NullCheckRule initialization"""
        rule = NullCheckRule()
        self.assertEqual(rule.rule_name, "null_check")
        self.assertIsNotNone(rule.db_manager)

    def test_init_with_db_manager(self):
        """Test NullCheckRule initialization with custom db_manager"""
        rule = NullCheckRule(db_manager=self.mock_db_manager)
        self.assertEqual(rule.rule_name, "null_check")
        self.assertEqual(rule.db_manager, self.mock_db_manager)

    @patch('pandas.read_sql')
    def test_validate_single_column_success(self, mock_read_sql):
        """Test successful validation with no NULL values"""
        # Setup mock data - no NULL values
        mock_result = pd.DataFrame({
            'total_rows': [1000],
            'null_count': [0]
        })
        mock_read_sql.return_value = mock_result

        result = self.null_check_rule._validate_single_column(
            self.mock_engine, 
            "demand.egon_demandregio_hh", 
            "demand"
        )

        # Verify result
        self.assertEqual(result['status'], 'SUCCESS')
        self.assertEqual(result['table'], 'demand.egon_demandregio_hh')
        self.assertEqual(result['column'], 'demand')
        self.assertEqual(result['total_rows'], 1000)
        self.assertEqual(result['null_count'], 0)
        self.assertEqual(result['invalid_count'], 0)
        self.assertEqual(result['check_type'], 'null')
        self.assertIn('No NULL values found', result['details'])

        # Verify SQL query was called
        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        self.assertEqual(call_args[0][1], self.mock_engine)  # engine argument
        self.assertIn('demand.egon_demandregio_hh', call_args[0][0])  # table in query
        self.assertIn('demand IS NULL', call_args[0][0])  # column in query

    @patch('pandas.read_sql')
    def test_validate_single_column_failure(self, mock_read_sql):
        """Test validation failure with NULL values found"""
        # Setup mock data - has NULL values
        mock_result = pd.DataFrame({
            'total_rows': [1000],
            'null_count': [15]
        })
        mock_read_sql.return_value = mock_result

        result = self.null_check_rule._validate_single_column(
            self.mock_engine, 
            "supply.egon_power_plants", 
            "el_capacity"
        )

        # Verify result
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['table'], 'supply.egon_power_plants')
        self.assertEqual(result['column'], 'el_capacity')
        self.assertEqual(result['total_rows'], 1000)
        self.assertEqual(result['null_count'], 15)
        self.assertEqual(result['invalid_count'], 15)
        self.assertEqual(result['check_type'], 'null')
        self.assertIn('Found 15 NULL values', result['details'])

    @patch('pandas.read_sql')
    def test_validate_single_column_sql_exception(self, mock_read_sql):
        """Test handling of SQL execution errors"""
        # Setup mock to raise exception
        mock_read_sql.side_effect = Exception("Table does not exist")

        result = self.null_check_rule._validate_single_column(
            self.mock_engine, 
            "nonexistent.table", 
            "column"
        )

        # Verify error handling
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['table'], 'nonexistent.table')
        self.assertEqual(result['column'], 'column')
        self.assertEqual(result['total_rows'], 0)
        self.assertEqual(result['null_count'], -1)
        self.assertEqual(result['invalid_count'], -1)
        self.assertEqual(result['check_type'], 'null')
        self.assertIn('SQL execution failed', result['details'])
        self.assertIn('Table does not exist', result['details'])

    @patch('pandas.read_sql')
    def test_validate_multiple_columns_success(self, mock_read_sql):
        """Test batch validation with multiple columns - all pass"""
        # Setup mock context manager
        mock_engine = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_engine)
        mock_context.__exit__ = Mock(return_value=None)
        self.mock_db_manager.connection_context.return_value = mock_context

        # Setup mock data - all columns pass
        mock_result = pd.DataFrame({
            'total_rows': [1000],
            'null_count': [0]
        })
        mock_read_sql.return_value = mock_result

        # Test configuration
        config = [
            {"table": "demand.egon_demandregio_hh", "column": "demand"},
            {"table": "demand.egon_demandregio_hh", "column": "nuts3"},
            {"table": "supply.egon_power_plants", "column": "el_capacity"}
        ]

        result = self.null_check_rule.validate(config)

        # Verify result
        self.assertIsInstance(result, ValidationResult)
        self.assertEqual(result.status, 'SUCCESS')
        self.assertEqual(result.rule_name, 'null_check')
        self.assertEqual(result.table, 'multiple_tables')
        self.assertIn('All 3 validations passed', result.message)
        
        # Verify detailed context
        self.assertEqual(result.detailed_context['total_validations'], 3)
        self.assertEqual(result.detailed_context['passed'], 3)
        self.assertEqual(result.detailed_context['failed'], 0)
        self.assertEqual(len(result.detailed_context['detailed_results']), 3)

    @patch('pandas.read_sql')
    def test_validate_multiple_columns_partial_failure(self, mock_read_sql):
        """Test batch validation with some failures"""
        # Setup mock context manager
        mock_engine = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_engine)
        mock_context.__exit__ = Mock(return_value=None)
        self.mock_db_manager.connection_context.return_value = mock_context

        # Setup mock data - first call succeeds, second fails
        mock_results = [
            pd.DataFrame({'total_rows': [1000], 'null_count': [0]}),    # Success
            pd.DataFrame({'total_rows': [1000], 'null_count': [5]}),    # Failure
            pd.DataFrame({'total_rows': [1000], 'null_count': [0]})     # Success
        ]
        mock_read_sql.side_effect = mock_results

        # Test configuration
        config = [
            {"table": "demand.egon_demandregio_hh", "column": "demand"},
            {"table": "demand.egon_demandregio_hh", "column": "nuts3"},
            {"table": "supply.egon_power_plants", "column": "el_capacity"}
        ]

        result = self.null_check_rule.validate(config)

        # Verify result
        self.assertIsInstance(result, ValidationResult)
        self.assertEqual(result.status, 'CRITICAL_FAILURE')
        self.assertEqual(result.rule_name, 'null_check')
        self.assertEqual(result.table, 'multiple_tables')
        self.assertIn('1 of 3 validations failed', result.error_details)
        self.assertIn('demand.egon_demandregio_hh.nuts3', result.error_details)
        
        # Verify detailed context
        self.assertEqual(result.detailed_context['total_validations'], 3)
        self.assertEqual(result.detailed_context['passed'], 2)
        self.assertEqual(result.detailed_context['failed'], 1)
        self.assertEqual(len(result.detailed_context['failed_tables']), 1)

    def test_validate_empty_config(self):
        """Test validation with empty configuration"""
        # Setup mock context manager
        mock_engine = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_engine)
        mock_context.__exit__ = Mock(return_value=None)
        self.mock_db_manager.connection_context.return_value = mock_context

        result = self.null_check_rule.validate([])

        # Verify result
        self.assertIsInstance(result, ValidationResult)
        self.assertEqual(result.status, 'SUCCESS')
        self.assertEqual(result.message, 'All 0 validations passed')
        self.assertEqual(result.detailed_context['total_validations'], 0)

    def test_validate_db_connection_error(self):
        """Test handling of database connection errors"""
        # Setup mock to raise exception on connection
        self.mock_db_manager.connection_context.side_effect = Exception("Database connection failed")

        config = [{"table": "test.table", "column": "test_column"}]
        result = self.null_check_rule.validate(config)

        # Verify error handling
        self.assertIsInstance(result, ValidationResult)
        self.assertEqual(result.status, 'CRITICAL_FAILURE')
        self.assertIn('Database connection failed', result.error_details)

    def test_sql_query_generation(self):
        """Test that SQL query is generated correctly"""
        with patch('pandas.read_sql') as mock_read_sql:
            mock_result = pd.DataFrame({
                'total_rows': [100],
                'null_count': [0]
            })
            mock_read_sql.return_value = mock_result

            self.null_check_rule._validate_single_column(
                self.mock_engine, 
                "test.schema.table", 
                "test_column"
            )

            # Verify SQL query structure
            call_args = mock_read_sql.call_args
            query = call_args[0][0]
            
            # Check query components
            self.assertIn('COUNT(*) as total_rows', query)
            self.assertIn('COUNT(CASE WHEN test_column IS NULL THEN 1 END) as null_count', query)
            self.assertIn('FROM test.schema.table', query)
            self.assertIn('LIMIT 10000', query)


if __name__ == '__main__':
    unittest.main()