
"""
Helper script to run the Nominatim spider with address list, optional proxy,
and proper output options.
"""
import argparse
import subprocess
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Run Nominatim spider with address list')
    parser.add_argument('--addresses', '-a', required=True, help='Path to file containing addresses (one per line)')
    parser.add_argument('--proxy', '-p', help='Proxy URL in format http://user:pass@host:port')
    parser.add_argument('--output', '-o', default='json', help='Output format (json, csv, xml)')
    parser.add_argument('--output-file', '-f', default='nominatim_results', help='Output filename (without extension)')
    parser.add_argument('--direct-output', '-d', action='store_true', help='Use Scrapy\'s built-in output option (bypasses pipeline)')
    
    args = parser.parse_args()
    
    # Verify address file exists
    if not os.path.exists(args.addresses):
        print(f"Error: Address file '{args.addresses}' not found")
        return 1
    
    # Create results directory if using pipeline output
    if not args.direct_output:
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        print(f"Created/verified 'results' directory at: {results_dir.absolute()}")
    
    # Build the scrapy command
    cmd = ['scrapy', 'crawl', 'nominatim', 
           '-a', f'address_file={args.addresses}']
    
    # Add proxy if specified
    if args.proxy:
        cmd.extend(['-a', f'proxy={args.proxy}'])
    
    # Add direct output option if requested
    if args.direct_output:
        output_path = f"{args.output_file}.{args.output}"
        cmd.extend(['-o', output_path])
        print(f"Using Scrapy's direct output option: {output_path}")
    else:
        print("Using pipeline for output (results/geocode_results.json)")
    
    print(f"Running command: {' '.join(cmd)}")
    
    # Run the spider
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print(f"Spider completed successfully.")
        print(f"Logs saved to nominatim_logger.log")
        
        # Check if the output file exists
        if args.direct_output:
            output_path = f"{args.output_file}.{args.output}"
            if os.path.exists(output_path):
                size = os.path.getsize(output_path)
                print(f"Output saved to {output_path} ({size} bytes)")
            else:
                print(f"Warning: Output file {output_path} not found!")
        else:
            pipeline_output = "results/geocode_results.json"
            if os.path.exists(pipeline_output):
                size = os.path.getsize(pipeline_output)
                print(f"Pipeline output saved to {pipeline_output} ({size} bytes)")
            else:
                print(f"Warning: Pipeline output file {pipeline_output} not found!")
                print("Try running with --direct-output flag as a fallback")
    else:
        print(f"Spider failed with exit code {result.returncode}")
    
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())