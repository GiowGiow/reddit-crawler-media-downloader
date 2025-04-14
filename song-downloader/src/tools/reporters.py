"""
Results reporting for the Suno Downloader.
"""

import json
from typing import Any, Dict


def print_download_summary(results: Dict[str, Any]) -> None:
    """
    Print a summary of download results.

    Args:
        results: Dictionary with download statistics
    """
    total = results["success"] + results["failed"] + results["skipped"]

    print("\n==================================================")
    print("                 DOWNLOAD SUMMARY                  ")
    print("==================================================")
    print(f"Total processed: {total}")

    if total > 0:
        print(
            f"Successfully downloaded: {results['success']} ({results['success']/total:.1%})"
        )
        print(f"Failed: {results['failed']} ({results['failed']/total:.1%})")
        print(
            f"Skipped (already exists): {results['skipped']} ({results['skipped']/total:.1%})"
        )
    else:
        print("No files were processed.")


def save_report(results: Dict[str, Any], report_path: str) -> None:
    """
    Save download results to a JSON file.

    Args:
        results: Dictionary with download statistics
        report_path: Path to save the report
    """
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDownload report saved to: {report_path}")
