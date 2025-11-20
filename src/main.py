import logging
import sys
from src.config.config import load_config
from src.server.main import Server


def setup_logging(log_level: str):
    """Configure logging for the application"""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main():
    """
    Main entry point for the Dataset Generation Service.
    Loads configuration and starts the server.
    """
    try:
        # Load configuration from environment variables
        config = load_config()

        # Setup logging
        setup_logging(config.log_level)

        logger = logging.getLogger(__name__)
        logger.info("=" * 60)
        logger.info("Dataset Generator Service")
        logger.info("=" * 60)

        # Create and start server
        server = Server(config)
        server.run()

    except Exception as e:
        logging.error(f"Failed to start service: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
