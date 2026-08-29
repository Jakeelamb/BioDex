//! One merge boundary for all deterministic, bundled species supplements.

use crate::curated_animals::apply_curated_animal_supplement;
use crate::species::UnifiedSpecies;
use crate::tree_of_sex::apply_tree_of_sex_supplement;

pub fn apply_local_supplements(species: &mut UnifiedSpecies) {
    apply_curated_animal_supplement(species);
    apply_tree_of_sex_supplement(species);
}
