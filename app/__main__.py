"""Allow running as: python -m app (delegates to main.main)."""

from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    from main import main
    main()
