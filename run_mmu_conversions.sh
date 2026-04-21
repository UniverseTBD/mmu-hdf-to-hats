#!/bin/bash

BASE_URL="https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1"
OUTPUT_DIR="./hats"
TMP_DIR="./tmp"
MAX_ROWS=8192

run_conv() {
    local transformer=$1
    local subpath=$2
    local name=$3
    
    echo "----------------------------------------------------------------"
    echo "Processing: $name (Transformer: $transformer)"
    echo "Input: $BASE_URL/$subpath"
    echo "----------------------------------------------------------------"
    
    uv run python ./main.py \
      --transformer="$transformer" \
      --input="$BASE_URL/$subpath" \
      --output="$OUTPUT_DIR" \
      --name="$name" \
      --tmp-dir="$TMP_DIR" \
      --max-rows=$MAX_ROWS
      
    if [ $? -ne 0 ]; then
        echo "Error processing $name. Skipping to next..."
    fi
}

# 1. CSP (~1.203MB)
run_conv "csp" "csp/csp/" "mmu_csp"

# 2. Swift SNE IA (~1.855MB)
run_conv "swift_sne_ia" "swift_sne_ia/data/" "mmu_swift_sne_ia"

# 3. CFA (~3.771MB)
run_conv "cfa" "cfa/cfa3/" "mmu_cfa"

# 4. SNLS (~4.309MB)
run_conv "snls" "snls/data/" "mmu_snls"

# 5. PS1 SNE IA (~5.345MB)
run_conv "ps1_sne_ia" "ps1_sne_ia/ps1_sne_ia/" "mmu_ps1_sne_ia"

# 6. DES Y3 SNE IA (~5.816MB)
run_conv "des_y3_sne_ia" "des_y3_sne_ia/des_y3_sne_ia/" "mmu_des_y3_sne_ia"

7. YSE (~53.52MB)
run_conv "yse" "yse/yse_dr1/" "mmu_yse"

# 8. CHANDRA (~236.7MB)
run_conv "chandra" "chandra/spectra/" "mmu_chandra"

# 9. VIPERS (~784.8MB)
run_conv "vipers" "vipers/vipers_w1/" "mmu_vipers"

# 10. DESI PROVABGS (~2.550GB)
run_conv "desi_provabgs" "desi_provabgs/datafiles/" "mmu_desi_provabgs"

# # 11. GZ10 (~3.251GB)
run_conv "gz10" "gz10/datafiles/" "mmu_gz10"

# 12. BTSBOT (~36.70GB)
run_conv "btsbot" "btsbot/data/" "mmu_btsbot"

# 13. PLASTICC (~65.62GB)
run_conv "plasticc" "plasticc/data/" "mmu_plasticc"

# 14. JWST (~131.9GB)
run_conv "jwst" "jwst/primer-cosmos/" "mmu_jwst"

# 15. GAIA (~172.4GB)
run_conv "gaia" "gaia/gaia/" "mmu_gaia"

# 16. TESS (~181.6GB)
run_conv "tess" "tess/spoc/" "mmu_tess"

# 17. SDSS (~301.5GB)
run_conv "sdss" "sdss/sdss/" "mmu_sdss_sdss"

# 18. HSC (~400.9GB)
run_conv "hsc" "hsc/pdr3_dud_22.5/" "mmu_hsc"

# 19. DESI (~498.8GB)
run_conv "desi" "desi/edr_sv3/" "mmu_desi"

echo "All conversions finished."
