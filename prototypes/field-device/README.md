# BioDex field-device UI prototype

Throwaway visual study for the question: **what should BioDex look and feel like as a rugged, future xenobiology field instrument?**

Open it with:

```bash
xdg-open /home/jake/Projects/bio_dex/prototypes/field-device/index.html
```

Use the floating switcher or the left/right arrow keys to compare:

- `?variant=A` — optic-first rugged field instrument
- `?variant=B` — salvaged split-console
- `?variant=C` — minimal expedition field slate

This prototype is intentionally disposable. It has no persistence and is not production BioDex code.

## Decision captured

Variant A is the selected direction. The dedicated Genome page was rejected; assembly chromosome count and mitochondrial genome size belong in the primary record alongside reproductive biology. The production design must distinguish assembly chromosome sequences from cytogenetic karyotype and sampled-individual sex from a species-level sex-determination system. It must not infer an unknown system, flatten real variation, or hide provenance.
