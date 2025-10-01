"""Command-line interface for BIXI scraper."""

import logging
import sys
from pathlib import Path
from typing import Optional

import click

from . import BixiAgent


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """BIXI website scraper for RAG and vector search."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    setup_logging(verbose)


@cli.command()
@click.option(
    "--output-dir", "-o", type=click.Path(), help="Output directory for scraped files"
)
@click.option(
    "--databricks-volume",
    "-d",
    type=str,
    help="Databricks volume path (e.g., /Volumes/main/bixi_data)",
)
@click.option(
    "--max-depth", "-m", type=int, default=3, help="Maximum crawl depth (default: 3)"
)
@click.option(
    "--delay",
    type=float,
    default=1.0,
    help="Delay between requests in seconds (default: 1.0)",
)
@click.option(
    "--start-url",
    "-u",
    default="https://bixi.com",
    help="Starting URL to scrape from (default: https://bixi.com)",
)
@click.pass_context
def scrape(
    ctx: click.Context,
    output_dir: Optional[str],
    databricks_volume: Optional[str],
    max_depth: int,
    delay: float,
    start_url: str,
) -> None:
    """Scrape BIXI website and save as markdown files."""

    if not output_dir and not databricks_volume:
        output_dir = "./scraped_data"
        click.echo(f"No output specified, using default: {output_dir}")

    if output_dir and databricks_volume:
        click.echo("Error: Cannot specify both --output-dir and --databricks-volume")
        sys.exit(1)

    try:
        # Initialize agent
        agent = BixiAgent(
            output_path=output_dir,
            databricks_volume=databricks_volume,
            max_depth=max_depth,
            delay=delay,
        )

        click.echo(f"Starting BIXI scraping...")
        click.echo(f"Start URL: {start_url}")
        click.echo(f"Max depth: {max_depth}")
        click.echo(f"Delay: {delay}s")

        if output_dir:
            click.echo(f"Output directory: {output_dir}")
        if databricks_volume:
            click.echo(f"Databricks volume: {databricks_volume}")

        # Perform scraping
        scraped_content = agent.scrape_website(start_url)

        # Show statistics
        stats = agent.get_scraping_stats()
        click.echo("\n" + "=" * 50)
        click.echo("SCRAPING COMPLETED")
        click.echo("=" * 50)
        click.echo(f"Total pages scraped: {stats['total_pages']}")
        click.echo(f"Successful pages: {stats['successful_pages']}")
        click.echo(f"Error pages: {stats['error_pages']}")
        click.echo(f"Total links found: {stats['total_links_found']}")
        click.echo(f"URLs visited: {stats['visited_urls']}")

        if stats["error_pages"] > 0:
            click.echo(f"\nWarning: {stats['error_pages']} pages had errors")

        click.echo("\nScraping completed successfully!")

    except KeyboardInterrupt:
        click.echo("\nScraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during scraping: {e}")
        if ctx.obj.get("verbose"):
            import traceback

            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option(
    "--input-dir",
    "-i",
    type=click.Path(exists=True),
    required=True,
    help="Directory containing scraped markdown files",
)
def analyze(input_dir: str) -> None:
    """Analyze scraped content and show statistics."""
    input_path = Path(input_dir)

    if not input_path.exists():
        click.echo(f"Error: Directory {input_dir} does not exist")
        sys.exit(1)

    # Find all markdown files
    md_files = list(input_path.glob("*.md"))

    if not md_files:
        click.echo(f"No markdown files found in {input_dir}")
        return

    click.echo(f"Found {len(md_files)} markdown files")

    # Analyze file sizes
    total_size = sum(f.stat().st_size for f in md_files)
    avg_size = total_size / len(md_files) if md_files else 0

    click.echo(f"Total size: {total_size:,} bytes")
    click.echo(f"Average file size: {avg_size:,.0f} bytes")

    # Show largest files
    largest_files = sorted(md_files, key=lambda f: f.stat().st_size, reverse=True)[:5]
    click.echo("\nLargest files:")
    for f in largest_files:
        size = f.stat().st_size
        click.echo(f"  {f.name}: {size:,} bytes")


def main() -> None:
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()

