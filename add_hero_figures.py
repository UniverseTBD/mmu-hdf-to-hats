import os
import re
import io
import random
import argparse
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
from PIL import Image
import importlib
import datasets
import matplotlib.pyplot as plt
import gc
from huggingface_hub import hf_hub_download, list_repo_files

def get_dataset_type(readme_path):
    content = readme_path.read_text().lower()
    if "rgb_image" in content or "image.flux" in content or "image triplets" in content or "array" in content:
        return "image"
    if "spectrum.flux" in content or "spectral_coefficients" in content or "stellar spectra" in content:
        return "spectrum"
    if "lightcurve.flux" in content or "lightcurve.mag" in content or "time-series" in content:
        return "lightcurve"
    if "posterior" in content or "mcmc" in content:
        return "posterior"
    return "unknown"

def sample_from_hf(dataset_name):
    try:
        print(f"  Sampling from HF: UniverseTBD/{dataset_name}")
        repo_id = f"UniverseTBD/{dataset_name}"
        
        # Try to find a parquet file first as it's more efficient to sample
        files = list_repo_files(repo_id=repo_id, repo_type="dataset")
        parquet_files = [f for f in files if f.endswith(".parquet") and "dataset" in f]
        if not parquet_files:
            parquet_files = [f for f in files if f.endswith(".parquet")]
            
        if parquet_files:
            target_file = sorted(parquet_files)[0]
            print(f"    Downloading single parquet file for sampling: {target_file}")
            local_path = hf_hub_download(repo_id=repo_id, filename=target_file, repo_type="dataset")
            
            parquet_file = pq.ParquetFile(local_path)
            # Read only the first row of the first batch
            batch = next(parquet_file.iter_batches(batch_size=1))
            if len(batch) > 0:
                row = batch.to_pylist()[0]
                return row

        # Fallback to streaming datasets if no parquet found or failed
        ds = datasets.load_dataset(repo_id, split="train", streaming=True)
        for i, row in enumerate(ds):
            if row:
                return row
            if i > 10: break
    except Exception as e:
        print(f"  HF Sample Error for {dataset_name}: {e}")
    return None

def visualize_image(row, output_path):
    try:
        # Check for 'array' as seen in btsbot
        if "array" in row:
            data = np.array(row["array"])
            # If it's (C, H, W) or (H, W, C)
            if data.ndim == 3:
                # btsbot usually has 3 images: science, ref, diff
                img_array = data[0]
                plt.figure(figsize=(6, 6))
                img_array = np.nan_to_num(img_array)
                plt.imshow(np.arcsinh(img_array), cmap='magma', origin='lower')
                plt.axis('off')
                plt.savefig(output_path, bbox_inches='tight', pad_inches=0)
                plt.close()
                return True

        if "rgb_image" in row:
            data = row["rgb_image"]
            if isinstance(data, dict) and "bytes" in data and data["bytes"]:
                img = Image.open(io.BytesIO(data["bytes"]))
                img.save(output_path)
                return True
            elif hasattr(data, "save"): 
                data.save(output_path)
                return True
        
        if "image" in row and isinstance(row["image"], dict):
            img_struct = row["image"]
            if "flux" in img_struct:
                flux = np.array(img_struct["flux"])
                if flux.ndim == 3: 
                    # For JWST style images
                    img_array = flux[len(flux)//2]
                    plt.figure(figsize=(6, 6))
                    img_array = np.nan_to_num(img_array)
                    plt.imshow(np.arcsinh(img_array), cmap='magma', origin='lower')
                    plt.axis('off')
                    plt.savefig(output_path, bbox_inches='tight', pad_inches=0)
                    plt.close()
                    return True
            # Alternative format
            elif "image.flux" in row:
                flux = np.array(row["image.flux"])
                if flux.ndim == 3:
                    img_array = flux[len(flux)//2]
                    plt.figure(figsize=(6, 6))
                    img_array = np.nan_to_num(img_array)
                    plt.imshow(np.arcsinh(img_array), cmap='magma', origin='lower')
                    plt.axis('off')
                    plt.savefig(output_path, bbox_inches='tight', pad_inches=0)
                    plt.close()
                    return True

    except Exception as e:
        print(f"  Image Visualization Error: {e}")
    return False

def visualize_spectrum(row, output_path):
    try:
        flux = None
        if "spectrum" in row and isinstance(row["spectrum"], dict):
            flux = np.array(row["spectrum"]["flux"])
        elif "spectrum.flux" in row:
            flux = np.array(row["spectrum.flux"])
        elif "spectral_coefficients.coeff" in row:
            flux = np.array(row["spectral_coefficients.coeff"])
        elif "spectral_coefficients" in row and isinstance(row["spectral_coefficients"], dict):
            flux = np.array(row["spectral_coefficients"]["coeff"])
        
        if flux is None or len(flux) == 0: 
            return False
        
        plt.figure(figsize=(10, 4))
        plt.plot(flux, color='#1f77b4', lw=1)
        plt.xlabel("Pixel Index")
        plt.ylabel("Flux")
        plt.title(f"Sample Spectrum")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_path, dpi=100)
        plt.close()
        return True
    except Exception as e:
        print(f"  Spectrum Visualization Error: {e}")
    return False

def visualize_lightcurve(row, output_path):
    try:
        lc = row.get("lightcurve", {})
        if not isinstance(lc, dict): return False
        
        time = np.array(lc.get("time", []))
        mag = lc.get("mag")
        flux = lc.get("flux")
        val = np.array(mag if mag is not None else flux)
        bands = np.array(lc.get("band", []))
        
        if len(time) == 0: return False

        plt.figure(figsize=(10, 4))
        if len(bands) > 0:
            for b in np.unique(bands):
                mask = bands == b
                plt.scatter(time[mask], val[mask], label=str(b), s=10)
        else:
            plt.scatter(time, val, s=10)
            
        if mag is not None: plt.gca().invert_yaxis()
        plt.xlabel("Time")
        plt.ylabel("Magnitude" if mag is not None else "Flux")
        plt.title("Sample Light Curve")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_path, dpi=100)
        plt.close()
        return True
    except Exception as e:
        print(f"  LC Visualization Error: {e}")
    return False

def visualize_posterior(row, output_path):
    try:
        data = None
        if "PROVABGS_MCMC" in row:
            # Take a sample or the whole chain if it's not too big
            data = np.array(row["PROVABGS_MCMC"])
            if data.ndim == 2: # (samples, parameters)
                plt.figure(figsize=(10, 4))
                for i in range(min(data.shape[1], 5)): # Plot first 5 parameters
                    plt.plot(data[:, i], alpha=0.5, label=f"Param {i}")
                plt.xlabel("Sample Index")
                plt.ylabel("Value")
                plt.title("MCMC Posterior Samples")
                plt.legend()
                plt.tight_layout()
                plt.savefig(output_path, dpi=100)
                plt.close()
                return True
        return False
    except Exception as e:
        print(f"  Posterior Visualization Error: {e}")
    return False

def update_readme(readme_path, image_filename):
    content = readme_path.read_text()
    img_tag = f'\n<div align="center">\n<img src="{image_filename}" width="600">\n</div>\n'
    if image_filename in content: return
    fm_match = re.search(r"^---\n(.*?)\n---\n", content, re.DOTALL)
    insertion_point = fm_match.end() if fm_match else 0
    new_content = content[:insertion_point] + img_tag + content[insertion_point:]
    readme_path.write_text(new_content)
    print(f"  Updated README.")

def main():
    for subdir in sorted(Path("readmes").iterdir()):
        if not subdir.is_dir(): continue
        readme_path = subdir / "README.md"
        if not readme_path.exists(): continue

        output_image = subdir / "example_figure.png"
        if output_image.exists() and "example_figure.png" in readme_path.read_text():
            print(f"Skipping {subdir.name}, hero figure already exists.")
            continue

        print(f"\nProcessing {subdir.name}...")
        dtype = get_dataset_type(readme_path)
        row = sample_from_hf(subdir.name)
        if not row: continue

        output_image = subdir / "example_figure.png"
        success = False
        if dtype == "image": success = visualize_image(row, output_image)
        elif dtype == "spectrum": success = visualize_spectrum(row, output_image)
        elif dtype == "lightcurve": success = visualize_lightcurve(row, output_image)
        elif dtype == "posterior": success = visualize_posterior(row, output_image)

        
        if success: update_readme(readme_path, "example_figure.png")
        else: print(f"  Visualization failed for {dtype}")
        
        gc.collect()

if __name__ == "__main__":
    main()
