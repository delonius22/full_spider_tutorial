"""
Helper script to run the Nominatim spider with address list and optional proxy.
"""
import argparse
import subprocess
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Run Nominatim spider with address list')
    parser.add_argument('--addresses', '-a', required=True, help='Path to file containing addresses (one per line)')
    parser.add_argument('--proxy', '-p', help='Proxy URL in format http://user:pass@host:port')
    parser.add_argument('--output', '-o', default='json', help='Output format (json, csv, xml)')
    parser.add_argument('--output-file', '-f', default='nominatim_results', help='Output filename (without extension)')
    
    args = parser.parse_args()
    
    # Verify address file exists
    if not os.path.exists(args.addresses):
        print(f"Error: Address file '{args.addresses}' not found")
        return 1
    
    # Build the scrapy command
    cmd = ['scrapy', 'crawl', 'nominatim', 
           '-a', f'address_file={args.addresses}']
    
    # Add proxy if specified
    if args.proxy:
        cmd.extend(['-a', f'proxy={args.proxy}'])
    
    # Add output format
    output_path = f"{args.output_file}.{args.output}"
    cmd.extend(['-o', output_path])
    
    print(f"Running command: {' '.join(cmd)}")
    
    # Run the spider
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print(f"Spider completed successfully. Results saved to {output_path}")
        print(f"Logs saved to nominatim_logger.log")
    else:
        print(f"Spider failed with exit code {result.returncode}")
    
    return result.returncode


if __name__ == "__main__":
    exit(main())