import pandas as pd
from typing import Dict, List, Set, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import json

from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger
from src.config.validation_config import VALIDATION_CONFIGURATIONS
from src.utils.template_loader import TemplateLoader


@dataclass
class TableInfo:
    """Information about a database table"""
    schema: str
    table: str
    column_count: int
    columns: List[str]
    estimated_row_count: int


@dataclass
class ValidationCoverage:
    """Validation coverage information for a table/column"""
    table: str
    column: str
    validation_types: List[str]  # e.g., ['null_check', 'time_series']
    configurations: List[str]  # e.g., ['comprehensive', 'critical_only']


class ValidationMonitor:
    """
    Monitor and analyze validation coverage across database schemas

    Provides:
    - Schema discovery and table analysis
    - Validation coverage matrix
    - HTML reports
    - Airflow integration data
    """

    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        self.logger = ValidationLogger("monitor")
        self.discovered_tables: List[TableInfo] = []
        self.validation_coverage: List[ValidationCoverage] = []

    def discover_database_structure(self) -> Dict[str, Any]:
        """
        Discover all schemas, tables, and columns in the database

        Returns:
        --------
        Dict with database structure information
        """
        self.logger.info("🔍 Starting database structure discovery")

        try:
            with self.db_manager.connection_context() as engine:
                # Query to get all tables with basic info
                discovery_query = """
                SELECT 
                    schemaname as schema_name,
                    tablename as table_name,
                    schemaname || '.' || tablename as full_table_name
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schemaname, tablename;
                """

                tables_df = pd.read_sql(discovery_query, engine)

                # Get detailed info for each table
                discovered_tables = []
                total_tables = len(tables_df)

                print(f"📊 Discovered {total_tables} tables across schemas")

                for idx, row in tables_df.iterrows():
                    schema = row['schema_name']
                    table = row['table_name']
                    full_table = row['full_table_name']

                    print(f"   [{idx + 1}/{total_tables}] Analyzing {full_table}")

                    # Get column information
                    columns_query = f"""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                    ORDER BY ordinal_position;
                    """

                    try:
                        columns_df = pd.read_sql(columns_query, engine)
                        columns = columns_df['column_name'].tolist()

                        # Get estimated row count (fast approximation)
                        row_count_query = f"""
                        SELECT reltuples::bigint as estimate
                        FROM pg_class 
                        WHERE relname = '{table}' AND relnamespace = (
                            SELECT oid FROM pg_namespace WHERE nspname = '{schema}'
                        );
                        """

                        row_count_df = pd.read_sql(row_count_query, engine)
                        estimated_rows = int(row_count_df.iloc[0]['estimate']) if len(row_count_df) > 0 else 0

                        table_info = TableInfo(
                            schema=schema,
                            table=full_table,
                            column_count=len(columns),
                            columns=columns,
                            estimated_row_count=estimated_rows
                        )

                        discovered_tables.append(table_info)

                        print(f"      ✅ {len(columns)} columns, ~{estimated_rows:,} rows")

                    except Exception as e:
                        print(f"      ❌ Error analyzing {full_table}: {str(e)}")
                        self.logger.warning(f"Failed to analyze table {full_table}: {str(e)}")
                        continue

                self.discovered_tables = discovered_tables

                # Summary statistics
                total_columns = sum(t.column_count for t in discovered_tables)
                schemas = set(t.schema for t in discovered_tables)

                summary = {
                    "discovery_timestamp": datetime.now().isoformat(),
                    "total_schemas": len(schemas),
                    "total_tables": len(discovered_tables),
                    "total_columns": total_columns,
                    "schemas": sorted(list(schemas)),
                    "tables": [asdict(t) for t in discovered_tables]
                }

                print(f"\n📈 Discovery Summary:")
                print(f"   Schemas: {len(schemas)}")
                print(f"   Tables: {len(discovered_tables)}")
                print(f"   Columns: {total_columns}")

                return summary

        except Exception as e:
            self.logger.critical(f"Database structure discovery failed: {str(e)}")
            raise

    def analyze_validation_coverage(self) -> Dict[str, Any]:
        """
        Analyze which tables/columns are covered by validation configurations

        Returns:
        --------
        Dict with coverage analysis
        """
        self.logger.info("🔍 Analyzing validation coverage")

        if not self.discovered_tables:
            raise ValueError("No tables discovered. Run discover_database_structure() first.")

        # Extract validations from comprehensive configuration only
        all_validations = []
        validation_by_table_column = {}

        # Only analyze comprehensive configuration
        config_name = "comprehensive"
        if config_name not in VALIDATION_CONFIGURATIONS:
            raise ValueError(f"Configuration '{config_name}' not found in VALIDATION_CONFIGURATIONS")
        
        config = VALIDATION_CONFIGURATIONS[config_name]
        print(f"\n📋 Analyzing configuration: {config_name}")

        for rule in config["rules"]:
            rule_name = rule["name"]
            rule_class = rule["rule_class"].__name__

            # Handle list of table/column configs
            if isinstance(rule["config"], list):
                for item in rule["config"]:
                    table = item["table"]
                    column = item["column"]

                    key = f"{table}.{column}"

                    if key not in validation_by_table_column:
                        validation_by_table_column[key] = {
                            "table": table,
                            "column": column,
                            "validation_types": set(),
                            "configurations": set()
                        }

                    validation_by_table_column[key]["validation_types"].add(rule_class)
                    validation_by_table_column[key]["configurations"].add(config_name)

                    print(f"   ✅ {table}.{column} → {rule_class}")

        # Convert to ValidationCoverage objects
        self.validation_coverage = []
        for key, data in validation_by_table_column.items():
            coverage = ValidationCoverage(
                table=data["table"],
                column=data["column"],
                validation_types=sorted(list(data["validation_types"])),
                configurations=sorted(list(data["configurations"]))
            )
            self.validation_coverage.append(coverage)

        # Coverage statistics
        covered_tables = set(c.table for c in self.validation_coverage)
        total_discovered_tables = set(t.table for t in self.discovered_tables)
        uncovered_tables = total_discovered_tables - covered_tables

        total_columns = sum(t.column_count for t in self.discovered_tables)
        covered_columns = len(self.validation_coverage)

        coverage_stats = {
            "analysis_timestamp": datetime.now().isoformat(),
            "total_discovered_tables": len(total_discovered_tables),
            "covered_tables": len(covered_tables),
            "uncovered_tables": len(uncovered_tables),
            "total_columns": total_columns,
            "covered_columns": covered_columns,
            "coverage_percentage": (covered_columns / total_columns * 100) if total_columns > 0 else 0,
            "covered_table_list": sorted(list(covered_tables)),
            "uncovered_table_list": sorted(list(uncovered_tables)),
            "validation_details": [asdict(c) for c in self.validation_coverage]
        }

        print(f"\n📊 Coverage Summary:")
        print(f"   Tables with validation: {len(covered_tables)}/{len(total_discovered_tables)}")
        print(f"   Columns with validation: {covered_columns}/{total_columns}")
        print(f"   Coverage: {coverage_stats['coverage_percentage']:.1f}%")

        if uncovered_tables:
            print(f"\n❌ Uncovered tables:")
            for table in sorted(uncovered_tables):
                print(f"   • {table}")

        return coverage_stats

    def generate_coverage_matrix_html(self, output_path: str = "validation_coverage_matrix.html") -> str:
        """
        Generate HTML coverage matrix report using template system

        Parameters:
        -----------
        output_path : str
            Path to save HTML report

        Returns:
        --------
        str : Path to generated HTML file
        """
        self.logger.info(f"📄 Generating HTML coverage matrix: {output_path}")

        if not self.discovered_tables or not self.validation_coverage:
            raise ValueError(
                "No data available. Run discover_database_structure() and analyze_validation_coverage() first.")

        # Initialize template loader
        template_loader = TemplateLoader()
        
        # Copy CSS and JavaScript files to output directory
        import os
        output_dir = os.path.dirname(output_path)
        if not output_dir:
            output_dir = "."
        template_loader.copy_css_to_output("validation_report.css", output_dir)
        template_loader.copy_js_to_output("validation_report.js", output_dir)

        # Prepare data for template
        covered_tables = set(c.table for c in self.validation_coverage)
        total_discovered_tables = set(t.table for t in self.discovered_tables)
        uncovered_tables = total_discovered_tables - covered_tables
        total_columns = sum(t.column_count for t in self.discovered_tables)
        covered_columns = len(self.validation_coverage)
        coverage_percentage = (covered_columns / total_columns * 100) if total_columns > 0 else 0

        # Get all validation types
        all_validation_types = set()
        for c in self.validation_coverage:
            all_validation_types.update(c.validation_types)
        all_validation_types = sorted(list(all_validation_types))

        # Build uncovered tables section
        uncovered_tables_section = ""
        if uncovered_tables:
            # Build table list using partials
            table_list = ""
            for table in sorted(uncovered_tables):
                table_list += template_loader.render_partial("table_list_item.html", {"table_name": table})
            
            uncovered_tables_section = template_loader.render_partial("uncovered_tables_section.html", {
                "uncovered_count": len(uncovered_tables),
                "table_list": table_list
            })

        # Build validation type headers
        validation_type_headers = ""
        for validation_type in all_validation_types:
            validation_type_headers += template_loader.render_partial("validation_type_header.html", {
                "validation_type": validation_type
            })

        # Build table rows
        tables_by_schema = {}
        for table_info in self.discovered_tables:
            schema = table_info.schema
            if schema not in tables_by_schema:
                tables_by_schema[schema] = []
            tables_by_schema[schema].append(table_info)

        table_rows = ""
        for schema in sorted(tables_by_schema.keys()):
            schema_tables = tables_by_schema[schema]
            first_table = True

            for table_info in sorted(schema_tables, key=lambda x: x.table):
                # Schema column (only for first table in schema)
                schema_cell = ""
                if first_table:
                    schema_cell = template_loader.render_partial("schema_cell.html", {
                        "rowspan": len(schema_tables),
                        "schema": schema
                    })
                    first_table = False

                # Validation coverage columns
                table_validations = {}
                for c in self.validation_coverage:
                    if c.table == table_info.table:
                        for val_type in c.validation_types:
                            if val_type not in table_validations:
                                table_validations[val_type] = []
                            table_validations[val_type].append(c.column)

                validation_columns = ""
                for validation_type in all_validation_types:
                    if validation_type in table_validations:
                        columns = table_validations[validation_type]
                        column_names = ", ".join(columns)
                        column_names_display = "<br>".join([f"• {col}" for col in columns])
                        validation_columns += template_loader.render_partial("covered_cell.html", {
                            "column_count": len(columns),
                            "column_names": column_names,
                            "column_names_display": column_names_display
                        })
                    else:
                        validation_columns += template_loader.render_partial("not_covered_cell.html", {})

                # Render complete table row
                table_rows += template_loader.render_partial("table_row.html", {
                    "schema_cell": schema_cell,
                    "table_name": table_info.table.split('.')[-1],
                    "column_count": table_info.column_count,
                    "estimated_row_count": table_info.estimated_row_count,
                    "validation_columns": validation_columns
                })

        # Build configuration list
        configuration_list = ""
        for config_name, config in VALIDATION_CONFIGURATIONS.items():
            configuration_list += template_loader.render_partial("configuration_list_item.html", {
                "config_name": config_name,
                "description": config.get('description', 'No description')
            })

        # Prepare template context
        context = {
            "generation_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "covered_tables": len(covered_tables),
            "total_tables": len(total_discovered_tables),
            "covered_columns": covered_columns,
            "total_columns": total_columns,
            "coverage_percentage": coverage_percentage,
            "validation_types_count": len(all_validation_types),
            "uncovered_tables_section": uncovered_tables_section,
            "validation_type_headers": validation_type_headers,
            "table_rows": table_rows,
            "configuration_list": configuration_list
        }

        # Render template
        html_content = template_loader.render_template("validation_report.html", context)

        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"✅ HTML report generated: {output_path}")
        return output_path

    def get_airflow_ready_data(self) -> Dict[str, Any]:
        """
        Generate data structure optimized for Airflow SQL Check Operators

        Returns:
        --------
        Dict with Airflow-compatible validation configurations
        """
        self.logger.info("🚀 Preparing Airflow-compatible validation data")

        if not self.validation_coverage:
            raise ValueError("No validation coverage data. Run analyze_validation_coverage() first.")

        # Group validations by type for Airflow operators
        airflow_data = {
            "sql_column_checks": {},
            "sql_table_checks": {},
            "custom_checks": [],
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_validations": len(self.validation_coverage),
                "source_configurations": list(VALIDATION_CONFIGURATIONS.keys())
            }
        }

        # Group by table for column checks
        for coverage in self.validation_coverage:
            table = coverage.table
            column = coverage.column

            if table not in airflow_data["sql_column_checks"]:
                airflow_data["sql_column_checks"][table] = {}

            # Map validation types to Airflow check format
            checks = {}
            for val_type in coverage.validation_types:
                if val_type == "NullCheckRule":
                    checks["null_check"] = {"equal_to": 0}
                elif val_type == "TimeSeriesValidationRule":
                    # This would need custom SQL check
                    checks["custom_time_series"] = {"description": "Time series length validation"}

            if checks:
                airflow_data["sql_column_checks"][table][column] = checks

        print(f"🎯 Airflow data prepared:")
        print(f"   SQL Column Checks: {len(airflow_data['sql_column_checks'])} tables")
        print(f"   Total validations: {len(self.validation_coverage)}")

        return airflow_data

    def generate_full_report(self, output_dir: str = ".") -> Dict[str, str]:
        """
        Generate complete monitoring report (discovery + coverage + HTML)

        Parameters:
        -----------
        output_dir : str
            Directory to save report files

        Returns:
        --------
        Dict with paths to generated files
        """
        self.logger.info("📊 Generating complete validation monitoring report")

        # Run full analysis
        discovery_data = self.discover_database_structure()
        coverage_data = self.analyze_validation_coverage()
        airflow_data = self.get_airflow_ready_data()

        # Generate files
        import os
        os.makedirs(output_dir, exist_ok=True)

        # HTML report
        html_path = os.path.join(output_dir, "validation_coverage_matrix.html")
        self.generate_coverage_matrix_html(html_path)

        # JSON data files
        discovery_path = os.path.join(output_dir, "database_discovery.json")
        coverage_path = os.path.join(output_dir, "validation_coverage.json")
        airflow_path = os.path.join(output_dir, "airflow_validation_data.json")

        with open(discovery_path, 'w') as f:
            json.dump(discovery_data, f, indent=2)

        with open(coverage_path, 'w') as f:
            json.dump(coverage_data, f, indent=2)

        with open(airflow_path, 'w') as f:
            json.dump(airflow_data, f, indent=2)

        generated_files = {
            "html_report": html_path,
            "discovery_data": discovery_path,
            "coverage_data": coverage_path,
            "airflow_data": airflow_path
        }

        print(f"\n🎉 Complete report generated:")
        for file_type, path in generated_files.items():
            print(f"   {file_type}: {path}")

        return generated_files