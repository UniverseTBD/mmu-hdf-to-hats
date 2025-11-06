from datasets import IterableDataset
from datasets.iterable_dataset import (
    _BaseExamplesIterable,
    ExamplesIterable,
    ArrowExamplesIterable
)
from astropy.coordinates import SkyCoord
from astropy import units as u
import numpy as np
from typing import Iterator, Tuple, List
import h5py

class CrossMatchedExamplesIterable(_BaseExamplesIterable):
    """
    Iterable that performs on-the-fly crossmatching between datasets organised
    by HEALPix pixels.
    """

    def __init__(
        self,
        left_iterable: _BaseExamplesIterable,
        right_iterable: _BaseExamplesIterable,
        left_builder,
        right_builder,
        matching_radius: float = 1.0,
    ):
        super().__init__()
        self.left_iterable = left_iterable
        self.right_iterable = right_iterable
        self.left_builder = left_builder
        self.right_builder = right_builder
        self.matching_radius = matching_radius
        self._healpix_cache = {}

    def _load_healpix_catalog(self, builder, healpix):
        """Load minimal catalog data for a specific healpix pixel"""
        if (builder.config.name, healpix) in self._healpix_cache:
            return self._healpix_cache[(builder.config.name, healpix)]

        files = [f for f in builder.config.data_files["train"]
                 if f"healpix={healpix}" in f]

        if not files:
            return None

        with h5py.File(files[0], "r") as f:
            catalog = {
                "object_id": f["object_id"][:],
                "ra": f["ra"][:],
                "dec": f["dec"][:]
            }

        self._healpix_cache[(builder.config.name, healpix)] = catalog
        return catalog

    def __iter__(self) -> Iterator[Tuple[str, dict]]:
        """
        Iterate through left dataset, performing crossmatching with
        right dataset on the fly per healpix pixel
        """
        left_by_healpix = {}
        for key, example in self.left_iterable:
            healpix = example.get('healpix')
            if healpix not in left_by_healpix:
                left_by_healpix[healpix] = []
            left_by_healpix[healpix].append((key, example))
        
        # Process each HEALPix pixel
        for healpix, left_examples in left_by_healpix.items():
            # Load right catalog for this healpix
            right_cat = self._load_healpix_catalog(self.right_builder, healpix)
            
            if right_cat is None:
                continue
            
            # Build coordinate objects
            left_coords = SkyCoord(
                [ex[1]['ra'] for ex in left_examples],
                [ex[1]['dec'] for ex in left_examples],
                unit='deg'
            )
            right_coords = SkyCoord(
                right_cat['ra'], right_cat['dec'], unit='deg'
            )
            
            # Perform cross-matching
            idx, sep2d, _ = left_coords.match_to_catalog_sky(right_coords)
            mask = sep2d < self.matching_radius * u.arcsec
            
            # Yield matched examples
            for i, (left_key, left_example) in enumerate(left_examples):
                if mask[i]:
                    matched_idx = idx[i]
                    right_object_id = right_cat['object_id'][matched_idx]
                    
                    # Load the full right example
                    right_example = self._load_example(
                        self.right_builder, healpix, right_object_id
                    )
                    
                    # Merge examples
                    merged = {**left_example, **right_example}
                    merged['object_id'] = left_example['object_id']
                    merged['separation_arcsec'] = sep2d[i].arcsec
                    
                    yield f"{left_key}_x_{right_object_id}", merged
    
    def _load_example(self, builder, healpix, object_id):
        """Load a specific example from a dataset."""
        files = [f for f in builder.config.data_files['train'] 
                 if f'healpix={healpix}' in f]
        
        if not files:
            return {}
            
        # Use the builder's _generate_examples with specific object_id
        gen = builder._generate_examples(
            files=files, 
            object_ids=[[object_id]]
        )
        
        for _, example in gen:
            return example
        
        return {}
    
    def shuffle_data_sources(self, generator):
        return CrossMatchedExamplesIterable(
            self.left_iterable.shuffle_data_sources(generator),
            self.right_iterable.shuffle_data_sources(generator),
            self.left_builder,
            self.right_builder,
            self.matching_radius
        )
    
    def shard_data_sources(self, num_shards, index, contiguous=True):
        return CrossMatchedExamplesIterable(
            self.left_iterable.shard_data_sources(num_shards, index, contiguous),
            self.right_iterable.shard_data_sources(num_shards, index, contiguous),
            self.left_builder,
            self.right_builder,
            self.matching_radius
        )
    
    @property
    def num_shards(self):
        return min(self.left_iterable.num_shards, self.right_iterable.num_shards)


