from src.core.database_manager import DatabaseManager
from src.core.validation_monitor import ValidationMonitor


def main():
    """Main function to run monitoring"""

    print("🔍 eGon Data Validation Monitor")
    print("=" * 50)

    # Initialize monitor (uses your existing DB connection)
    print("📡 Initializing monitor...")
    monitor = ValidationMonitor()

    # Run complete analysis
    print("\n🔍 Running complete analysis...")
    try:
        # This does everything in one step:
        # - Database Structure Discovery
        # - Validation Coverage Analysis
        # - HTML Report Generation
        # - JSON Data Export

        report_files = monitor.generate_full_report(output_dir="./monitoring_reports")

        print("\n✅ Monitoring completed successfully!")
        print("\n📊 Generated files:")
        for file_type, path in report_files.items():
            print(f"   {file_type}: {path}")

        print(f"\n🌐 Open this in your browser:")
        print(f"   file://{abs_path(report_files['html_report'])}")

    except Exception as e:
        print(f"❌ Error during monitoring: {e}")
        print("\nTroubleshooting:")
        print("- Check your database connection (SSH tunnel active?)")
        print("- Verify your .env file contains correct DB credentials")
        print("- Ensure validation_config.py is properly configured")


def abs_path(relative_path):
    """Helper to get absolute path for browser opening"""
    import os
    return os.path.abspath(relative_path)


if __name__ == "__main__":
    main()
