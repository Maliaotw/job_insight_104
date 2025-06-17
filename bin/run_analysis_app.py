#!/usr/bin/env python
"""
Run Analysis App Script

This script starts the Streamlit analysis application for visualizing job data.
"""

import os
import sys
import subprocess
from pathlib import Path

# Add the parent directory to the path so we can import from src
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config_loader import ConfigLoader
from config.settings import logger


class AnalysisApp:
    """Analysis App Runner using Python Fire."""

    def run(self, port=8501, browser=False):
        """
        Start the Streamlit analysis application.

        Args:
            port (int): Port to run the Streamlit app on (default: 8501)
            config (str): Path to the configuration file
            browser (bool): Open the app in a browser (default: False)
        """

        # Get the path to the Streamlit analysis app
        app_path = Path(__file__).parent.parent / "apps" / "visualization" / "app.py"

        # Build the command to run Streamlit
        cmd = ["streamlit", "run", str(app_path), "--server.port", str(port)]

        # Add browser option if specified
        if not browser:
            cmd.extend(["--server.headless", "true"])

        # Set environment variables for configuration
        env = os.environ.copy()

        logger.info(f"Starting Streamlit analysis app from: {app_path}")
        logger.info(f"App will be available at: http://localhost:{port}")

        try:
            # Run the Streamlit app
            subprocess.run(cmd, env=env)
        except KeyboardInterrupt:
            logger.info("Stopping Streamlit analysis app...")
        except Exception as e:
            logger.error(f"Error running Streamlit analysis app: {e}")
            sys.exit(1)


def main():
    """Main function to start the Fire CLI."""
    app = AnalysisApp()
    app.run(port=8501, browser=True)


if __name__ == "__main__":
    main()
