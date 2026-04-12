//! Poketex-style TUI for species display
//!
//! Features real images, gauge-based stats, and adaptive layout.

use crate::api::gbif::GbifClient;
use crate::curated_animals::{canonical_curated_species_name, CURATED_ANIMAL_TARGET};
use crate::local_db::TaxonName;
use crate::service::SpeciesService;
use crate::species::{ImageInfo, UnifiedSpecies};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use futures::StreamExt;
pub use image::DynamicImage;
use ratatui::{
    prelude::*,
    widgets::{
        Block, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap,
    },
};
use ratatui_image::{picker::Picker, protocol::StatefulProtocol, Resize, StatefulImage};
use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant as TokioInstant};

// Color palette for type badges and UI elements
const TYPE_COLORS: &[(&str, Color)] = &[
    ("Animalia", Color::Rgb(108, 168, 164)),  // Muted teal
    ("Plantae", Color::Rgb(119, 221, 119)),   // Pastel green
    ("Fungi", Color::Rgb(186, 135, 89)),      // Brown
    ("Bacteria", Color::Rgb(135, 206, 235)),  // Sky blue
    ("Archaea", Color::Rgb(255, 179, 71)),    // Orange
    ("Protista", Color::Rgb(177, 156, 217)),  // Lavender
    ("Chromista", Color::Rgb(255, 218, 121)), // Gold
];

const SHELL_RED: Color = Color::Rgb(32, 36, 40);
const SHELL_EDGE: Color = Color::Rgb(95, 104, 112);
const SHELL_PANEL: Color = Color::Rgb(225, 230, 234);
const SCREEN_EDGE: Color = Color::Rgb(114, 141, 136);
const DATA_EDGE: Color = Color::Rgb(108, 145, 156);
const DATA_BG: Color = Color::Rgb(20, 24, 28);
const PANEL_BG: Color = Color::Rgb(27, 31, 35);
const HEADER_TEXT: Color = Color::Rgb(38, 42, 46);
const HEADER_MUTED: Color = Color::Rgb(102, 109, 116);
const ACCENT_YELLOW: Color = Color::Rgb(191, 201, 210);
const ACCENT_SKY: Color = Color::Rgb(127, 165, 175);
const ACCENT_MINT: Color = Color::Rgb(144, 172, 162);
const PANEL_BORDER: Color = Color::Rgb(74, 86, 97);

const SEARCH_DEBOUNCE_DELAY: Duration = Duration::from_millis(140);
const SPECIES_AUTO_OPEN_DELAY: Duration = Duration::from_millis(120);
const SEARCH_SUGGESTION_LIMIT: u32 = 50;
const TAXON_BROWSER_LIMIT: u32 = 500;
const SPECIES_LIST_LIMIT: u32 = CURATED_ANIMAL_TARGET as u32;
const TAXON_ALIASES: &[TaxonAlias] = &[
    TaxonAlias {
        query: "animals",
        scientific_name: "Animalia",
        display_name: "Animals",
        rank: "KINGDOM",
    },
    TaxonAlias {
        query: "plants",
        scientific_name: "Plantae",
        display_name: "Plants",
        rank: "KINGDOM",
    },
    TaxonAlias {
        query: "fungi",
        scientific_name: "Fungi",
        display_name: "Fungi",
        rank: "KINGDOM",
    },
    TaxonAlias {
        query: "mammals",
        scientific_name: "Mammalia",
        display_name: "Mammals",
        rank: "CLASS",
    },
    TaxonAlias {
        query: "birds",
        scientific_name: "Aves",
        display_name: "Birds",
        rank: "CLASS",
    },
    TaxonAlias {
        query: "reptiles",
        scientific_name: "Reptilia",
        display_name: "Reptiles",
        rank: "CLASS",
    },
    TaxonAlias {
        query: "lizards",
        scientific_name: "Squamata",
        display_name: "Lizards and snakes",
        rank: "ORDER",
    },
    TaxonAlias {
        query: "snakes",
        scientific_name: "Squamata",
        display_name: "Lizards and snakes",
        rank: "ORDER",
    },
    TaxonAlias {
        query: "amphibians",
        scientific_name: "Amphibia",
        display_name: "Amphibians",
        rank: "CLASS",
    },
    TaxonAlias {
        query: "fish",
        scientific_name: "Actinopterygii",
        display_name: "Ray-finned fishes",
        rank: "CLASS",
    },
    TaxonAlias {
        query: "insects",
        scientific_name: "Insecta",
        display_name: "Insects",
        rank: "CLASS",
    },
    TaxonAlias {
        query: "dinosaurs",
        scientific_name: "Dinosauria",
        display_name: "Dinosaurs",
        rank: "CLADE",
    },
];

fn get_kingdom_color(kingdom: &Option<String>) -> Color {
    kingdom
        .as_ref()
        .and_then(|k| TYPE_COLORS.iter().find(|(name, _)| name == k))
        .map(|(_, color)| *color)
        .unwrap_or(Color::Gray)
}

/// Sibling taxon for the sibling browser
#[derive(Debug, Clone)]
pub struct SiblingTaxon {
    pub name: String,
    pub rank: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NavigatorFocus {
    Taxonomy,
    SpeciesList,
}

#[derive(Debug, Clone)]
struct BrowserContext {
    name: String,
    rank: String,
}

#[derive(Debug, Clone)]
struct BrowserPaneState {
    entries: Vec<SiblingTaxon>,
    title: String,
    index: usize,
    context: Option<BrowserContext>,
}

/// Search suggestion for autocomplete
#[derive(Debug, Clone)]
pub struct SearchSuggestion {
    pub name: String,
    pub canonical_name: Option<String>,
    pub rank: String,
}

struct TaxonAlias {
    query: &'static str,
    scientific_name: &'static str,
    display_name: &'static str,
    rank: &'static str,
}

#[derive(Clone, Copy)]
enum StatusTone {
    Info,
    Success,
    Warning,
    Error,
}

struct StatusBanner {
    tone: StatusTone,
    message: String,
}

impl StatusBanner {
    fn new(tone: StatusTone, message: impl Into<String>) -> Self {
        Self {
            tone,
            message: message.into(),
        }
    }
}

struct PendingSearch {
    query: String,
    generation: u64,
    deadline: TokioInstant,
}

struct PendingSpeciesOpen {
    name: String,
    deadline: TokioInstant,
}

#[derive(Default)]
struct SearchRuntime {
    pending: Option<PendingSearch>,
    generation: u64,
    active_generation: Option<u64>,
    task: Option<JoinHandle<()>>,
}

impl SearchRuntime {
    fn cancel(&mut self) {
        self.pending = None;
        self.active_generation = None;
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
    }

    fn reset(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.cancel();
    }

    fn queue(
        &mut self,
        query: &str,
        has_offline_search: bool,
        search_suggestions: &mut Option<Vec<SearchSuggestion>>,
        loading: &mut bool,
    ) {
        self.generation = self.generation.wrapping_add(1);
        self.cancel();

        if has_offline_search && query.len() >= 2 {
            self.pending = Some(PendingSearch {
                query: query.to_string(),
                generation: self.generation,
                deadline: TokioInstant::now() + SEARCH_DEBOUNCE_DELAY,
            });
        } else {
            *search_suggestions = None;
        }

        *loading = false;
    }
}

#[derive(Default)]
struct SpeciesListRuntime {
    pending: Option<PendingSpeciesOpen>,
}

impl SpeciesListRuntime {
    fn cancel(&mut self) {
        self.pending = None;
    }

    fn queue(&mut self, selected_name: Option<&str>, current_name: &str) {
        let Some(name) = selected_name.filter(|name| !name.eq_ignore_ascii_case(current_name))
        else {
            self.pending = None;
            return;
        };

        self.pending = Some(PendingSpeciesOpen {
            name: name.to_string(),
            deadline: TokioInstant::now() + SPECIES_AUTO_OPEN_DELAY,
        });
    }
}

pub struct TuiBootstrap {
    pub species: UnifiedSpecies,
    pub species_image: Option<DynamicImage>,
    pub map_image: Option<DynamicImage>,
    pub is_favorite: bool,
    pub has_offline_search: bool,
}

pub struct TuiRuntime {
    pub update_tx: mpsc::Sender<TuiUpdate>,
    pub update_rx: mpsc::Receiver<TuiUpdate>,
    pub service: Arc<SpeciesService>,
    pub gbif: Arc<GbifClient>,
}

struct RenderState<'a> {
    species: &'a UnifiedSpecies,
    browser_entries: &'a [SiblingTaxon],
    browser_title: &'a str,
    browser_index: usize,
    species_list_entries: &'a [SiblingTaxon],
    species_list_index: usize,
    navigator_focus: NavigatorFocus,
    image_state: &'a mut Option<PortraitImageState>,
    map_state: &'a mut Option<MapImageState>,
    search_mode: bool,
    search_query: &'a str,
    search_suggestions: Option<&'a [SearchSuggestion]>,
    search_selected: usize,
    is_favorite: bool,
    has_offline_search: bool,
}

/// Data update message sent to TUI while running
pub enum TuiUpdate {
    /// New species data loaded
    SpeciesLoaded {
        species: Box<UnifiedSpecies>,
        refreshed: bool,
    },
    /// Media finished loading for the currently displayed species
    MediaLoaded {
        scientific_name: String,
        species_image: Option<DynamicImage>,
        map_image: Option<DynamicImage>,
    },
    /// Siblings data loaded
    SiblingsLoaded {
        taxa: Vec<SiblingTaxon>,
        title: String,
        context_name: Option<String>,
        context_rank: Option<String>,
        selected_name: Option<String>,
    },
    SpeciesListLoaded {
        entries: Vec<SiblingTaxon>,
        selected_name: Option<String>,
    },
    /// Search suggestions loaded
    SuggestionsLoaded {
        suggestions: Vec<SearchSuggestion>,
        generation: u64,
    },
    /// Favorite state updated
    FavoriteUpdated { is_favorite: bool },
    /// Loading failed with error message
    LoadError {
        message: String,
        requested_name: String,
        refreshed: bool,
    },
}

struct PortraitImageState {
    protocol: StatefulProtocol,
}

impl PortraitImageState {
    fn new(picker: Picker, image: DynamicImage) -> Self {
        Self {
            protocol: picker.new_resize_protocol(image),
        }
    }
}

struct MapImageState {
    picker: Picker,
    source: DynamicImage,
    rendered_area: Option<(u16, u16)>,
    protocol: StatefulProtocol,
}

impl MapImageState {
    fn new(picker: Picker, image: DynamicImage) -> Self {
        let protocol = picker.new_resize_protocol(image.clone());
        Self {
            picker,
            source: image,
            rendered_area: None,
            protocol,
        }
    }

    fn protocol_for_area(&mut self, area: Rect) -> &mut StatefulProtocol {
        let area_size = (area.width, area.height);
        if self.rendered_area != Some(area_size) {
            let stretched = crate::world_map::stretch_for_terminal_area(
                &self.source,
                area.width,
                area.height,
                self.picker.font_size(),
            );
            self.protocol = self.picker.new_resize_protocol(stretched);
            self.rendered_area = Some(area_size);
        }
        &mut self.protocol
    }
}

/// Main entry point - runs the TUI with background data fetching
pub async fn run_tui_loop(
    initial: TuiBootstrap,
    runtime: TuiRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    stdout().execute(EnterAlternateScreen)?;
    enable_raw_mode()?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    terminal.clear()?;

    let picker = Picker::from_query_stdio().ok();
    let TuiRuntime {
        update_tx,
        mut update_rx,
        service,
        gbif,
    } = runtime;

    // Mutable state that can be updated while running
    let mut species = initial.species;
    let mut species_image = initial.species_image;
    let mut map_image = initial.map_image;
    let mut browser_entries: Vec<SiblingTaxon> = Vec::new();
    let mut browser_title = String::from("Loading browser...");
    let mut browser_context: Option<BrowserContext> = None;
    let mut browser_history: Vec<BrowserPaneState> = Vec::new();
    let mut species_list_entries: Vec<SiblingTaxon> = Vec::new();
    let mut search_suggestions: Option<Vec<SearchSuggestion>> = None;
    let mut is_favorite = initial.is_favorite;
    let has_offline_search = initial.has_offline_search;
    let mut status = StatusBanner::new(
        StatusTone::Info,
        format!(
            "{} ready. Use ↑/↓ for the A-Z species list, or press t for taxonomy.",
            species.scientific_name
        ),
    );

    let mut image_state = picker
        .as_ref()
        .zip(species_image.as_ref())
        .map(|(p, i)| PortraitImageState::new(*p, i.clone()));
    let mut map_state = picker
        .as_ref()
        .zip(map_image.as_ref())
        .map(|(p, i)| MapImageState::new(*p, i.clone()));
    let mut show_help = false;
    let mut search_mode = false;
    let mut search_query = String::new();
    let mut search_selected: usize = 0;
    let mut browser_index: usize = 0;
    let mut species_list_index: usize = 0;
    let mut navigator_focus = NavigatorFocus::SpeciesList;
    let mut loading = false;
    let mut loading_start = Instant::now();
    let mut search_runtime = SearchRuntime::default();
    let mut species_list_runtime = SpeciesListRuntime::default();

    let mut event_stream = EventStream::new();
    spawn_browser_for_species(update_tx.clone(), service.clone(), species.clone());
    spawn_species_list(
        update_tx.clone(),
        service.clone(),
        selected_species_name(&species),
    );

    loop {
        let spinner_frame = if loading {
            (loading_start.elapsed().as_millis() / 80) as usize % 10
        } else {
            0
        };

        terminal.draw(|frame| {
            if show_help {
                render_help(frame);
            } else {
                let layout = Layout::vertical([Constraint::Min(0), Constraint::Length(3)])
                    .split(frame.area());
                let mut render_state = RenderState {
                    species: &species,
                    browser_entries: &browser_entries,
                    browser_title: &browser_title,
                    browser_index,
                    species_list_entries: &species_list_entries,
                    species_list_index,
                    navigator_focus,
                    image_state: &mut image_state,
                    map_state: &mut map_state,
                    search_mode,
                    search_query: &search_query,
                    search_suggestions: search_suggestions.as_deref(),
                    search_selected,
                    is_favorite,
                    has_offline_search,
                };
                render_frame(frame, layout[0], &mut render_state);
                render_status_bar(
                    frame,
                    layout[1],
                    &status,
                    loading,
                    spinner_frame,
                    is_favorite,
                    has_offline_search,
                );
            }
        })?;

        // Use tokio::select! to handle both keyboard events and data updates
        tokio::select! {
            // Check for keyboard events
            maybe_event = event_stream.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        // If in search mode
                        if search_mode {
                            match key.code {
                                KeyCode::Esc => {
                                    search_runtime.reset();
                                    species_list_runtime.cancel();
                                    search_mode = false;
                                    search_query.clear();
                                    search_suggestions = None;
                                    loading = false;
                                    status = StatusBanner::new(
                                        StatusTone::Info,
                                        "Search closed. Browse the taxonomy browser or open another entry.",
                                    );
                                }
                                KeyCode::Enter => {
                                    // Select highlighted suggestion or use raw query
                                    let selected_suggestion = search_suggestions
                                        .as_ref()
                                        .and_then(|suggestions| suggestions.get(search_selected))
                                        .cloned()
                                        .or_else(|| {
                                            canonical_curated_species_name(&search_query).map(|name| SearchSuggestion {
                                                name: name.to_string(),
                                                canonical_name: None,
                                                rank: "SPECIES".to_string(),
                                            })
                                        });
                                    let name_to_navigate = selected_suggestion
                                        .as_ref()
                                        .map(|s| s.name.clone());

                                    if let Some(name) = name_to_navigate {
                                        let selected_rank = selected_suggestion.as_ref().map(|s| s.rank.clone());
                                        let opening_species = selected_rank
                                            .as_deref()
                                            .is_none_or(is_species_rank);
                                        search_runtime.reset();
                                        species_list_runtime.cancel();
                                        search_mode = false;
                                        search_query.clear();
                                        search_suggestions = None;
                                        loading = true;
                                        loading_start = Instant::now();
                                        if opening_species {
                                            status = StatusBanner::new(
                                                StatusTone::Info,
                                                format!("Opening {}...", name),
                                            );
                                            let tx = update_tx.clone();
                                            let svc = service.clone();
                                            let gb = gbif.clone();
                                            tokio::spawn(async move {
                                                open_taxon_entry(tx, svc, gb, name, selected_rank).await;
                                            });
                                        } else if let Some(rank) = selected_rank {
                                            browser_history.clear();
                                            navigator_focus = NavigatorFocus::Taxonomy;
                                            status = StatusBanner::new(
                                                StatusTone::Info,
                                                format!("Browsing {}...", name),
                                            );
                                            spawn_browser_for_context(
                                                update_tx.clone(),
                                                service.clone(),
                                                BrowserContext { name, rank },
                                                None,
                                            );
                                        }
                                    } else {
                                        status = StatusBanner::new(
                                            StatusTone::Warning,
                                            "Search is limited to the curated FieldDex species and their cached taxonomy.",
                                        );
                                    }
                                }
                                KeyCode::Down | KeyCode::Tab => {
                                    if let Some(ref suggestions) = search_suggestions {
                                        if search_selected < suggestions.len().saturating_sub(1) {
                                            search_selected += 1;
                                        }
                                    }
                                }
                                KeyCode::Up | KeyCode::BackTab => {
                                    search_selected = search_selected.saturating_sub(1);
                                }
                                KeyCode::Backspace => {
                                    search_query.pop();
                                    search_selected = 0;
                                    search_runtime.queue(
                                        &search_query,
                                        has_offline_search,
                                        &mut search_suggestions,
                                        &mut loading,
                                    );
                                }
                                KeyCode::Char(c) => {
                                    search_query.push(c);
                                    search_selected = 0;
                                    search_runtime.queue(
                                        &search_query,
                                        has_offline_search,
                                        &mut search_suggestions,
                                        &mut loading,
                                    );
                                }
                                _ => {}
                            }
                            continue;
                        }

                        match key.code {
                            KeyCode::Char('q') => {
                                break;
                            }
                            KeyCode::Esc => {
                                if show_help {
                                    show_help = false;
                                } else {
                                    break;
                                }
                            }
                            KeyCode::Char('/') => {
                                search_runtime.reset();
                                species_list_runtime.cancel();
                                search_mode = true;
                                search_query.clear();
                                search_suggestions = None;
                                loading = false;
                                status = if has_offline_search {
                                    StatusBanner::new(
                                        StatusTone::Info,
                                        "Search the local taxonomy index, or type a full species name and press Enter.",
                                    )
                                } else {
                                    StatusBanner::new(
                                        StatusTone::Warning,
                                        "Offline search index not imported yet. Type a full name and press Enter.",
                                    )
                                };
                            }
                            KeyCode::Char('b') => {
                                browser_history.clear();
                                navigator_focus = NavigatorFocus::Taxonomy;
                                species_list_runtime.cancel();
                                loading = true;
                                loading_start = Instant::now();
                                status = StatusBanner::new(
                                    StatusTone::Info,
                                    "Opening cached kingdoms...",
                                );
                                spawn_root_browser(
                                    update_tx.clone(),
                                    service.clone(),
                                    species.taxonomy.kingdom.clone(),
                                );
                            }
                            KeyCode::Char('?') => show_help = !show_help,
                            KeyCode::Char('r') => {
                                loading = true;
                                loading_start = Instant::now();
                                status = StatusBanner::new(
                                    StatusTone::Info,
                                    format!("Refreshing {}...", species.scientific_name),
                                );
                                spawn_species_refresh(
                                    update_tx.clone(),
                                    service.clone(),
                                    gbif.clone(),
                                    species.scientific_name.clone(),
                                );
                            }
                            KeyCode::Char('f') => {
                                status = StatusBanner::new(
                                    StatusTone::Info,
                                    format!("Updating collection status for {}...", species.scientific_name),
                                );
                                let tx = update_tx.clone();
                                let svc = service.clone();
                                let name = species.scientific_name.clone();
                                tokio::spawn(async move {
                                    toggle_favorite(tx, svc, name).await;
                                });
                            }
                            KeyCode::Char('t') => {
                                navigator_focus = match navigator_focus {
                                    NavigatorFocus::Taxonomy => NavigatorFocus::SpeciesList,
                                    NavigatorFocus::SpeciesList => NavigatorFocus::Taxonomy,
                                };
                                if navigator_focus == NavigatorFocus::Taxonomy {
                                    species_list_runtime.cancel();
                                }
                                status = match navigator_focus {
                                    NavigatorFocus::Taxonomy => StatusBanner::new(
                                        StatusTone::Info,
                                        "Taxonomy browser active. Use ←/→ to walk the lineage.",
                                    ),
                                    NavigatorFocus::SpeciesList => StatusBanner::new(
                                        StatusTone::Info,
                                        "A-Z species list active. Use ↑/↓ to skim; pausing auto-loads the species.",
                                    ),
                                };
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                match navigator_focus {
                                    NavigatorFocus::Taxonomy => {
                                        if browser_index < browser_entries.len().saturating_sub(1) {
                                            browser_index += 1;
                                        }
                                    }
                                    NavigatorFocus::SpeciesList => {
                                        let previous = species_list_index;
                                        if species_list_index
                                            < species_list_entries.len().saturating_sub(1)
                                        {
                                            species_list_index += 1;
                                        }
                                        if species_list_index != previous {
                                            queue_selected_species_auto_open(
                                                &mut species_list_runtime,
                                                &species_list_entries,
                                                species_list_index,
                                                &species.scientific_name,
                                            );
                                        }
                                    }
                                }
                            }
                            KeyCode::Up | KeyCode::Char('k') => {
                                match navigator_focus {
                                    NavigatorFocus::Taxonomy => {
                                        browser_index = browser_index.saturating_sub(1);
                                    }
                                    NavigatorFocus::SpeciesList => {
                                        let previous = species_list_index;
                                        species_list_index = species_list_index.saturating_sub(1);
                                        if species_list_index != previous {
                                            queue_selected_species_auto_open(
                                                &mut species_list_runtime,
                                                &species_list_entries,
                                                species_list_index,
                                                &species.scientific_name,
                                            );
                                        }
                                    }
                                }
                            }
                            KeyCode::Right | KeyCode::Char('l') | KeyCode::Enter => {
                                species_list_runtime.cancel();
                                match navigator_focus {
                                    NavigatorFocus::Taxonomy => {
                                        if let Some(entry) = browser_entries.get(browser_index).cloned() {
                                            loading = true;
                                            loading_start = Instant::now();
                                            if is_species_rank(&entry.rank) {
                                                status = StatusBanner::new(
                                                    StatusTone::Info,
                                                    format!("Opening {}...", entry.name),
                                                );
                                                let tx = update_tx.clone();
                                                let svc = service.clone();
                                                let gb = gbif.clone();
                                                tokio::spawn(async move {
                                                    open_taxon_entry(tx, svc, gb, entry.name, Some(entry.rank)).await;
                                                });
                                            } else {
                                                browser_history.push(BrowserPaneState {
                                                    entries: browser_entries.clone(),
                                                    title: browser_title.clone(),
                                                    index: browser_index,
                                                    context: browser_context.clone(),
                                                });
                                                status = StatusBanner::new(
                                                    StatusTone::Info,
                                                    format!("Browsing {}...", entry.name),
                                                );
                                                let selected_name = preferred_child_selection(
                                                    &species,
                                                    &entry.name,
                                                    &entry.rank,
                                                );
                                                spawn_browser_for_context(
                                                    update_tx.clone(),
                                                    service.clone(),
                                                    BrowserContext {
                                                        name: entry.name,
                                                        rank: entry.rank,
                                                    },
                                                    selected_name,
                                                );
                                            }
                                        } else {
                                            status = StatusBanner::new(
                                                StatusTone::Warning,
                                                "No cached taxonomy entries are available here yet.",
                                            );
                                        }
                                    }
                                    NavigatorFocus::SpeciesList => {
                                        if let Some(entry) =
                                            species_list_entries.get(species_list_index).cloned()
                                        {
                                            loading = true;
                                            loading_start = Instant::now();
                                            status = StatusBanner::new(
                                                StatusTone::Info,
                                                format!("Opening {}...", entry.name),
                                            );
                                            let tx = update_tx.clone();
                                            let svc = service.clone();
                                            let gb = gbif.clone();
                                            tokio::spawn(async move {
                                                open_taxon_entry(tx, svc, gb, entry.name, Some(entry.rank)).await;
                                            });
                                        } else {
                                            status = StatusBanner::new(
                                                StatusTone::Warning,
                                                "No cached species are available in the list yet.",
                                            );
                                        }
                                    }
                                }
                            }
                            KeyCode::Left | KeyCode::Char('h') => {
                                if navigator_focus == NavigatorFocus::SpeciesList {
                                    species_list_runtime.cancel();
                                    status = StatusBanner::new(
                                        StatusTone::Info,
                                        "A-Z species list active. Press t to swap into taxonomy browsing.",
                                    );
                                } else if let Some(previous) = browser_history.pop() {
                                    browser_entries = previous.entries;
                                    browser_title = previous.title;
                                    browser_index = previous.index;
                                    browser_context = previous.context;
                                    loading = false;
                                    status = StatusBanner::new(
                                        StatusTone::Info,
                                        "Moved up one taxonomy step.",
                                    );
                                } else if let Some(context) = browser_context.clone() {
                                    loading = true;
                                    loading_start = Instant::now();
                                    status = StatusBanner::new(
                                        StatusTone::Info,
                                        format!("Moving up from {}...", context.name),
                                    );
                                    spawn_parent_browser(
                                        update_tx.clone(),
                                        service.clone(),
                                        context,
                                    );
                                } else {
                                    loading = true;
                                    loading_start = Instant::now();
                                    status = StatusBanner::new(
                                        StatusTone::Info,
                                        "Opening cached kingdoms...",
                                    );
                                    spawn_root_browser(
                                        update_tx.clone(),
                                        service.clone(),
                                        species.taxonomy.kingdom.clone(),
                                    );
                                }
                            }
                            KeyCode::Home | KeyCode::Char('g') => {
                                match navigator_focus {
                                    NavigatorFocus::Taxonomy => {
                                        browser_index = 0;
                                    }
                                    NavigatorFocus::SpeciesList => {
                                        let previous = species_list_index;
                                        species_list_index = 0;
                                        if species_list_index != previous {
                                            queue_selected_species_auto_open(
                                                &mut species_list_runtime,
                                                &species_list_entries,
                                                species_list_index,
                                                &species.scientific_name,
                                            );
                                        }
                                    }
                                }
                            }
                            KeyCode::End | KeyCode::Char('G') => {
                                match navigator_focus {
                                    NavigatorFocus::Taxonomy => {
                                        browser_index = browser_entries.len().saturating_sub(1);
                                    }
                                    NavigatorFocus::SpeciesList => {
                                        let previous = species_list_index;
                                        species_list_index =
                                            species_list_entries.len().saturating_sub(1);
                                        if species_list_index != previous {
                                            queue_selected_species_auto_open(
                                                &mut species_list_runtime,
                                                &species_list_entries,
                                                species_list_index,
                                                &species.scientific_name,
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(_)) => {} // Ignore other events
                    Some(Err(_)) => {} // Ignore errors
                    None => break, // Stream ended
                }
            }

            _ = async {
                if let Some(search) = search_runtime.pending.as_ref() {
                    tokio::time::sleep_until(search.deadline).await;
                }
            }, if search_runtime.pending.is_some() => {
                if let Some(PendingSearch { query, generation, .. }) = search_runtime.pending.take() {
                    search_runtime.active_generation = Some(generation);
                    loading = true;
                    loading_start = Instant::now();

                    let tx = update_tx.clone();
                    let svc = service.clone();
                    search_runtime.task = Some(tokio::spawn(async move {
                        fetch_suggestions_local(tx, svc, query, generation).await;
                    }));
                }
            }

            _ = async {
                if let Some(pending) = species_list_runtime.pending.as_ref() {
                    tokio::time::sleep_until(pending.deadline).await;
                }
            }, if species_list_runtime.pending.is_some() => {
                if let Some(PendingSpeciesOpen { name, .. }) = species_list_runtime.pending.take() {
                    loading = true;
                    loading_start = Instant::now();
                    status = StatusBanner::new(
                        StatusTone::Info,
                        format!("Opening {}...", name),
                    );
                    let tx = update_tx.clone();
                    let svc = service.clone();
                    let gb = gbif.clone();
                    tokio::spawn(async move {
                        open_taxon_entry(tx, svc, gb, name, Some("SPECIES".to_string())).await;
                    });
                }
            }

            // Check for data updates from the channel
            maybe_update = update_rx.recv() => {
                match maybe_update {
                    Some(TuiUpdate::SpeciesLoaded {
                        species: new_species,
                        refreshed,
                    }) => {
                        if refreshed
                            && !new_species
                                .scientific_name
                                .eq_ignore_ascii_case(&species.scientific_name)
                        {
                            continue;
                        }

                        species = *new_species;
                        is_favorite = service.is_favorite(&species.scientific_name).await;
                        image_state = None;
                        map_state = None;
                        loading = false;

                        search_suggestions = None;
                        if let Some(index) = species_list_entries
                            .iter()
                            .position(|entry| entry.name.eq_ignore_ascii_case(&species.scientific_name))
                        {
                            species_list_index = index;
                        }
                        spawn_species_list(
                            update_tx.clone(),
                            service.clone(),
                            selected_species_name(&species),
                        );
                        if let Some(index) = browser_entries
                            .iter()
                            .position(|entry| entry.name.eq_ignore_ascii_case(&species.scientific_name))
                        {
                            browser_index = index;
                        } else {
                            browser_entries.clear();
                            browser_title = "Loading browser...".to_string();
                            browser_index = 0;
                            browser_context = None;
                            browser_history.clear();
                            spawn_browser_for_species(update_tx.clone(), service.clone(), species.clone());
                        }
                        status = match (refreshed, navigator_focus) {
                            (true, NavigatorFocus::Taxonomy) => StatusBanner::new(
                                StatusTone::Success,
                                format!(
                                    "Refreshed {}. Taxonomy browser still active; press t for the A-Z list.",
                                    species.scientific_name
                                ),
                            ),
                            (true, NavigatorFocus::SpeciesList) => StatusBanner::new(
                                StatusTone::Success,
                                format!(
                                    "Refreshed {}. A-Z list active; pausing on another species auto-loads it.",
                                    species.scientific_name
                                ),
                            ),
                            (false, NavigatorFocus::Taxonomy) => StatusBanner::new(
                                StatusTone::Success,
                                format!(
                                    "Opened {}. Taxonomy browser still active; press t for the A-Z list.",
                                    species.scientific_name
                                ),
                            ),
                            (false, NavigatorFocus::SpeciesList) => StatusBanner::new(
                                StatusTone::Success,
                                format!(
                                    "Opened {}. Use ↑/↓ to browse; pausing on another species auto-loads it.",
                                    species.scientific_name
                                ),
                            ),
                        };
                    }
                    Some(TuiUpdate::MediaLoaded { scientific_name, species_image: new_img, map_image: new_map }) => {
                        if scientific_name == species.scientific_name {
                            species_image = new_img;
                            map_image = new_map;
                            image_state = picker
                                .as_ref()
                                .zip(species_image.as_ref())
                                .map(|(p, i)| PortraitImageState::new(*p, i.clone()));
                            map_state = picker
                                .as_ref()
                                .zip(map_image.as_ref())
                                .map(|(p, i)| MapImageState::new(*p, i.clone()));
                        }
                    }
                    Some(TuiUpdate::SpeciesListLoaded {
                        entries,
                        selected_name,
                    }) => {
                        species_list_index = selected_name
                            .as_deref()
                            .and_then(|name| {
                                entries
                                    .iter()
                                    .position(|entry| entry.name.eq_ignore_ascii_case(name))
                            })
                            .or_else(|| {
                                entries.iter().position(|entry| {
                                    entry.name.eq_ignore_ascii_case(&species.scientific_name)
                                })
                            })
                            .unwrap_or_else(|| {
                                species_list_index.min(entries.len().saturating_sub(1))
                            });
                        species_list_entries = entries;
                    }
                    Some(TuiUpdate::SiblingsLoaded {
                        taxa: new_siblings,
                        title,
                        context_name,
                        context_rank,
                        selected_name,
                    }) => {
                        browser_title = title;
                        browser_context = match (context_name, context_rank) {
                            (Some(name), Some(rank)) => Some(BrowserContext { name, rank }),
                            _ => None,
                        };
                        browser_index = selected_name
                            .as_deref()
                            .and_then(|name| {
                                new_siblings
                                    .iter()
                                    .position(|entry| entry.name.eq_ignore_ascii_case(name))
                            })
                            .or_else(|| {
                                new_siblings
                                    .iter()
                                    .position(|entry| entry.name.eq_ignore_ascii_case(&species.scientific_name))
                            })
                            .unwrap_or(0);
                        browser_entries = new_siblings;
                        if browser_entries.is_empty() {
                            status = StatusBanner::new(
                                StatusTone::Warning,
                                "No cached taxonomy entries were found here yet.",
                            );
                        }
                        loading = false;
                    }
                    Some(TuiUpdate::SuggestionsLoaded { suggestions: new_suggestions, generation }) => {
                        if search_mode && generation == search_runtime.generation {
                            if search_runtime.active_generation == Some(generation) {
                                search_runtime.task = None;
                                search_runtime.active_generation = None;
                            }

                            let suggestion_count = new_suggestions.len();
                            search_suggestions = Some(new_suggestions);
                            search_selected = 0;
                            loading = false;
                            if search_query.len() >= 2 {
                                status = if suggestion_count > 0 {
                                    StatusBanner::new(
                                        StatusTone::Success,
                                        format!("{suggestion_count} indexed matches ready. Press Enter to open one."),
                                    )
                                } else if has_offline_search {
                                    StatusBanner::new(
                                        StatusTone::Warning,
                                        format!("No indexed matches for \"{}\". Press Enter to search it live.", search_query),
                                    )
                                } else {
                                    StatusBanner::new(
                                        StatusTone::Warning,
                                        "Offline index unavailable. Press Enter to open the typed name directly.",
                                    )
                                };
                            }
                        }
                    }
                    Some(TuiUpdate::FavoriteUpdated { is_favorite: new_state }) => {
                        is_favorite = new_state;
                        status = if is_favorite {
                            StatusBanner::new(
                                StatusTone::Success,
                                format!("{} was added to your collection.", species.scientific_name),
                            )
                        } else {
                            StatusBanner::new(
                                StatusTone::Info,
                                format!("{} was removed from your collection.", species.scientific_name),
                            )
                        };
                    }
                    Some(TuiUpdate::LoadError {
                        message,
                        requested_name,
                        refreshed,
                    }) => {
                        if refreshed && !requested_name.eq_ignore_ascii_case(&species.scientific_name)
                        {
                            continue;
                        }
                        loading = false;
                        status = if refreshed {
                            StatusBanner::new(
                                StatusTone::Warning,
                                format!("Refresh failed for {}: {}", requested_name, message),
                            )
                        } else {
                            StatusBanner::new(StatusTone::Error, message)
                        };
                    }
                    None => {
                        // Channel closed, exit
                        break;
                    }
                }
            }

            // Timeout for animation when loading (50ms)
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)), if loading => {
                // Just continue to redraw with updated spinner
            }
        }
    }

    search_runtime.cancel();
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

/// Background task to fetch a species and send update
async fn fetch_species_background(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    name: String,
) {
    fetch_species_internal(tx, service, gbif, name, false).await;
}

/// Background task to force refresh a species from APIs
async fn fetch_species_background_refresh(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    name: String,
) {
    fetch_species_internal(tx, service, gbif, name, true).await;
}

/// Internal species fetch with optional force refresh
async fn fetch_species_internal(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    name: String,
    force_refresh: bool,
) {
    let fetch_span = crate::perf::start_span();
    if !force_refresh {
        if let Some(cached) = service.get_cached_with_images(&name).await {
            let species = cached.species;
            let (species_image, map_image) = crate::decode_cached_media(
                &service,
                &species,
                crate::local_db::CachedMedia {
                    species_image: cached.species_image,
                    map_image: cached.map_image,
                },
            )
            .await;

            let _ = tx
                .send(TuiUpdate::SpeciesLoaded {
                    species: Box::new(species.clone()),
                    refreshed: false,
                })
                .await;
            let _ = tx
                .send(TuiUpdate::MediaLoaded {
                    scientific_name: species.scientific_name.clone(),
                    species_image,
                    map_image,
                })
                .await;
            crate::perf::log_value("tui.cached_species_open", &species.scientific_name);
            crate::perf::log_elapsed("tui.fetch_species_open", fetch_span);
            return;
        }
    }

    match service.lookup_with_options(&name, force_refresh).await {
        Ok(new_species) => {
            let _ = tx
                .send(TuiUpdate::SpeciesLoaded {
                    species: Box::new(new_species.clone()),
                    refreshed: force_refresh,
                })
                .await;

            if force_refresh {
                fetch_media_background(tx.clone(), service, gbif, new_species, force_refresh).await;
            } else {
                load_cached_media_background(tx.clone(), service, new_species).await;
            }

            crate::perf::log_elapsed(
                if force_refresh {
                    "tui.fetch_species_refresh"
                } else {
                    "tui.fetch_species_open"
                },
                fetch_span,
            );
        }
        Err(e) => {
            crate::perf::log_elapsed(
                if force_refresh {
                    "tui.fetch_species_refresh"
                } else {
                    "tui.fetch_species_open"
                },
                fetch_span,
            );
            let _ = tx
                .send(TuiUpdate::LoadError {
                    message: e.to_string(),
                    requested_name: name,
                    refreshed: force_refresh,
                })
                .await;
        }
    }
}

fn spawn_species_refresh(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    name: String,
) {
    tokio::spawn(async move {
        fetch_species_background_refresh(tx, service, gbif, name).await;
    });
}

async fn load_cached_media_background(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    species: UnifiedSpecies,
) {
    let (species_image, map_image) = crate::load_cached_media(&service, &species).await;

    let _ = tx
        .send(TuiUpdate::MediaLoaded {
            scientific_name: species.scientific_name.clone(),
            species_image,
            map_image,
        })
        .await;
}

/// Toggle favorite status for a species
async fn toggle_favorite(tx: mpsc::Sender<TuiUpdate>, service: Arc<SpeciesService>, name: String) {
    let is_favorite = service.toggle_favorite(&name).await;
    let _ = tx.send(TuiUpdate::FavoriteUpdated { is_favorite }).await;
}

pub async fn fetch_media_background(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    species: UnifiedSpecies,
    force_refresh: bool,
) {
    let (species_image, map_image) = tokio::join!(
        crate::download_species_image(&species, &service),
        crate::download_map_image_with_options(&gbif, &species, &service, force_refresh),
    );

    let _ = tx
        .send(TuiUpdate::MediaLoaded {
            scientific_name: species.scientific_name.clone(),
            species_image,
            map_image,
        })
        .await;
}

fn child_rank_for(rank: &str) -> Option<&'static str> {
    match rank.to_ascii_uppercase().as_str() {
        "KINGDOM" => Some("PHYLUM"),
        "PHYLUM" => Some("CLASS"),
        "CLASS" => Some("ORDER"),
        "ORDER" => Some("FAMILY"),
        "FAMILY" => Some("GENUS"),
        "GENUS" => Some("SPECIES"),
        _ => None,
    }
}

fn is_species_rank(rank: &str) -> bool {
    rank.eq_ignore_ascii_case("SPECIES")
}

fn root_taxa() -> Vec<SiblingTaxon> {
    TYPE_COLORS
        .iter()
        .map(|(name, _)| SiblingTaxon {
            name: (*name).to_string(),
            rank: "KINGDOM".to_string(),
        })
        .collect()
}

fn taxonomy_value_for_rank(species: &UnifiedSpecies, rank: &str) -> Option<String> {
    match rank.to_ascii_uppercase().as_str() {
        "KINGDOM" => species.taxonomy.kingdom.clone(),
        "PHYLUM" => species.taxonomy.phylum.clone(),
        "CLASS" => species.taxonomy.class.clone(),
        "ORDER" => species.taxonomy.order.clone(),
        "FAMILY" => species.taxonomy.family.clone(),
        "GENUS" => species.taxonomy.genus.clone(),
        "SPECIES" => Some(species.scientific_name.clone()),
        _ => None,
    }
}

fn species_within_taxon(species: &UnifiedSpecies, taxon_name: &str, taxon_rank: &str) -> bool {
    taxonomy_value_for_rank(species, taxon_rank)
        .is_some_and(|value| value.eq_ignore_ascii_case(taxon_name))
}

fn preferred_child_selection(
    species: &UnifiedSpecies,
    taxon_name: &str,
    taxon_rank: &str,
) -> Option<String> {
    if !species_within_taxon(species, taxon_name, taxon_rank) {
        return None;
    }

    let child_rank = child_rank_for(taxon_rank)?;
    taxonomy_value_for_rank(species, child_rank)
}

fn browser_context_for_species(species: &UnifiedSpecies) -> Option<BrowserContext> {
    if is_species_rank(&species.rank) {
        return species.taxonomy.genus.clone().map(|name| BrowserContext {
            name,
            rank: "GENUS".to_string(),
        });
    }

    child_rank_for(&species.rank)?;
    Some(BrowserContext {
        name: species.scientific_name.clone(),
        rank: species.rank.clone(),
    })
}

fn browser_request_for_context(
    context: &BrowserContext,
) -> Option<(String, String, String, String)> {
    let child_rank = child_rank_for(&context.rank)?.to_string();
    let title = format!("{} in {}", display_rank(&child_rank), context.name);
    Some((
        context.rank.clone(),
        context.name.clone(),
        child_rank,
        title,
    ))
}

fn split_browser_context(context: Option<BrowserContext>) -> (Option<String>, Option<String>) {
    match context {
        Some(context) => (Some(context.name), Some(context.rank)),
        None => (None, None),
    }
}

fn taxa_to_browser_entries(taxa: Vec<TaxonName>) -> Vec<SiblingTaxon> {
    taxa.into_iter()
        .map(|taxon| SiblingTaxon {
            name: taxon.canonical_name.unwrap_or(taxon.scientific_name),
            rank: taxon.rank,
        })
        .collect()
}

fn selected_species_name(species: &UnifiedSpecies) -> Option<String> {
    if is_species_rank(&species.rank) {
        Some(species.scientific_name.clone())
    } else {
        None
    }
}

fn queue_selected_species_auto_open(
    runtime: &mut SpeciesListRuntime,
    entries: &[SiblingTaxon],
    selected_index: usize,
    current_name: &str,
) {
    runtime.queue(
        entries.get(selected_index).map(|entry| entry.name.as_str()),
        current_name,
    );
}

async fn send_browser_entries(
    tx: mpsc::Sender<TuiUpdate>,
    entries: Vec<SiblingTaxon>,
    title: String,
    context: Option<BrowserContext>,
    selected_name: Option<String>,
) {
    let (context_name, context_rank) = split_browser_context(context);
    let _ = tx
        .send(TuiUpdate::SiblingsLoaded {
            taxa: entries,
            title,
            context_name,
            context_rank,
            selected_name,
        })
        .await;
}

fn spawn_browser_for_species(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    species: UnifiedSpecies,
) {
    tokio::spawn(async move {
        fetch_browser_for_species(tx, service, species).await;
    });
}

fn spawn_browser_for_context(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    context: BrowserContext,
    selected_name: Option<String>,
) {
    tokio::spawn(async move {
        fetch_browser_for_context(tx, service, context, selected_name).await;
    });
}

fn spawn_root_browser(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    selected_name: Option<String>,
) {
    tokio::spawn(async move {
        fetch_root_browser(tx, service, selected_name).await;
    });
}

fn spawn_species_list(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    selected_name: Option<String>,
) {
    tokio::spawn(async move {
        fetch_species_list(tx, service, selected_name).await;
    });
}

fn spawn_parent_browser(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    context: BrowserContext,
) {
    tokio::spawn(async move {
        fetch_parent_browser(tx, service, context).await;
    });
}

async fn fetch_browser_for_species(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    species: UnifiedSpecies,
) {
    let Some(context) = browser_context_for_species(&species) else {
        fetch_root_browser(tx, service, species.taxonomy.kingdom.clone()).await;
        return;
    };

    let Some((parent_rank, parent_value, child_rank, title)) =
        browser_request_for_context(&context)
    else {
        fetch_root_browser(tx, service, species.taxonomy.kingdom.clone()).await;
        return;
    };

    let mut taxa = service
        .get_siblings_local(
            &parent_rank,
            &parent_value,
            &child_rank,
            TAXON_BROWSER_LIMIT,
        )
        .await;

    if taxa.is_empty() && is_species_rank(&species.rank) {
        if let Some(gbif_key) = species.ids.gbif_key {
            taxa = service
                .get_species_batch_after(gbif_key, TAXON_BROWSER_LIMIT)
                .await;
            if taxa.is_empty() {
                taxa = service
                    .get_species_batch_after(0, TAXON_BROWSER_LIMIT)
                    .await;
            }
            send_browser_entries(
                tx,
                taxa_to_browser_entries(taxa),
                "Next cached species".to_string(),
                None,
                Some(species.scientific_name),
            )
            .await;
            return;
        }
    }

    send_browser_entries(
        tx,
        taxa_to_browser_entries(taxa),
        title,
        Some(context),
        Some(species.scientific_name),
    )
    .await;
}

async fn fetch_browser_for_context(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    context: BrowserContext,
    selected_name: Option<String>,
) {
    let Some((parent_rank, parent_value, child_rank, title)) =
        browser_request_for_context(&context)
    else {
        fetch_root_browser(tx, service, selected_name).await;
        return;
    };

    let taxa = service
        .get_siblings_local(
            &parent_rank,
            &parent_value,
            &child_rank,
            TAXON_BROWSER_LIMIT,
        )
        .await;

    send_browser_entries(
        tx,
        taxa_to_browser_entries(taxa),
        title,
        Some(context),
        selected_name,
    )
    .await;
}

async fn fetch_root_browser(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    selected_name: Option<String>,
) {
    let kingdoms = service.get_cached_kingdoms().await;
    let entries = if kingdoms.is_empty() {
        root_taxa()
    } else {
        kingdoms
            .into_iter()
            .map(|name| SiblingTaxon {
                name,
                rank: "KINGDOM".to_string(),
            })
            .collect()
    };

    send_browser_entries(tx, entries, "Kingdoms".to_string(), None, selected_name).await;
}

async fn fetch_species_list(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    selected_name: Option<String>,
) {
    let entries = service
        .get_cached_species_names(SPECIES_LIST_LIMIT)
        .await
        .into_iter()
        .map(|name| SiblingTaxon {
            name,
            rank: "SPECIES".to_string(),
        })
        .collect();

    let _ = tx
        .send(TuiUpdate::SpeciesListLoaded {
            entries,
            selected_name,
        })
        .await;
}

async fn fetch_parent_browser(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    context: BrowserContext,
) {
    if let Some((name, rank)) = service
        .get_cached_parent_taxon(&context.rank, &context.name)
        .await
    {
        fetch_browser_for_context(
            tx,
            service,
            BrowserContext { name, rank },
            Some(context.name),
        )
        .await;
        return;
    }

    fetch_root_browser(tx, service, Some(context.name)).await;
}

async fn open_taxon_entry(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    name: String,
    _rank: Option<String>,
) {
    fetch_species_background(tx, service, gbif, name).await;
}

fn alias_suggestions(query: &str) -> Vec<SearchSuggestion> {
    let normalized = query.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Vec::new();
    }

    TAXON_ALIASES
        .iter()
        .filter(|alias| alias.query.contains(&normalized) || normalized.contains(alias.query))
        .map(|alias| SearchSuggestion {
            name: alias.scientific_name.to_string(),
            canonical_name: Some(alias.display_name.to_string()),
            rank: alias.rank.to_string(),
        })
        .collect()
}

/// Fetch search suggestions from local database (instant - no network call)
async fn fetch_suggestions_local(
    tx: mpsc::Sender<TuiUpdate>,
    service: Arc<SpeciesService>,
    query: String,
    generation: u64,
) {
    let search_span = crate::perf::start_span();
    if query.len() < 2 {
        let _ = tx
            .send(TuiUpdate::SuggestionsLoaded {
                suggestions: Vec::new(),
                generation,
            })
            .await;
        return;
    }

    let taxa = service
        .search_offline(&query, SEARCH_SUGGESTION_LIMIT)
        .await;
    let mut results: Vec<SearchSuggestion> = alias_suggestions(&query);
    results.extend(taxa.into_iter().map(|t| SearchSuggestion {
        name: t.scientific_name,
        canonical_name: t.canonical_name,
        rank: t.rank,
    }));
    results.sort_by(|a, b| a.name.cmp(&b.name));
    results.dedup_by(|a, b| {
        a.name.eq_ignore_ascii_case(&b.name) && a.rank.eq_ignore_ascii_case(&b.rank)
    });
    results.truncate(SEARCH_SUGGESTION_LIMIT as usize);

    let _ = tx
        .send(TuiUpdate::SuggestionsLoaded {
            suggestions: results,
            generation,
        })
        .await;
    crate::perf::log_elapsed("tui.search_suggestions", search_span);
}

fn render_frame(frame: &mut Frame, area: Rect, state: &mut RenderState<'_>) {
    let shell = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(SHELL_EDGE))
        .style(Style::default().bg(SHELL_RED))
        .title(" FieldDex ")
        .title_style(Style::default().fg(Color::Black).bg(ACCENT_YELLOW).bold());
    let inner = shell.inner(area);
    frame.render_widget(shell, area);

    let rows = Layout::vertical([Constraint::Length(4), Constraint::Min(0)])
        .margin(1)
        .split(inner);
    render_top_banner(
        frame,
        rows[0],
        state.species,
        state.is_favorite,
        state.has_offline_search,
    );

    if rows[1].width >= 100 {
        render_wide_layout(frame, rows[1], state);
    } else if rows[1].width >= 60 {
        render_medium_layout(frame, rows[1], state);
    } else {
        render_narrow_layout(frame, rows[1], state);
    }

    if state.search_mode {
        render_search_overlay(
            frame,
            area,
            state.search_query,
            state.search_suggestions,
            state.search_selected,
            state.has_offline_search,
        );
    }
}

fn render_top_banner(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    is_favorite: bool,
    has_offline_search: bool,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(SHELL_EDGE))
        .style(Style::default().bg(SHELL_PANEL));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let cols = Layout::horizontal([Constraint::Min(20), Constraint::Length(24)]).split(inner);
    let common_name = primary_common_name(species);
    let display_name = common_name.unwrap_or(&species.scientific_name);
    let display_name_style = if common_name.is_some() {
        Style::default().fg(HEADER_TEXT).bold()
    } else {
        scientific_name_style(&species.rank, Style::default().fg(HEADER_TEXT).bold())
    };
    let catalog_id = primary_catalog_id(species);
    let rank_label = display_rank(&species.rank);

    let left_lines = vec![
        Line::from(vec![
            badge("FIELD", Color::Black, ACCENT_YELLOW),
            Span::raw(" "),
            Span::styled(
                trim_for_line(display_name, cols[0].width.saturating_sub(10) as usize),
                display_name_style,
            ),
        ]),
        Line::from(Span::styled(
            trim_for_line(&species.scientific_name, cols[0].width as usize),
            scientific_name_style(&species.rank, Style::default().fg(HEADER_MUTED)),
        )),
        Line::from(vec![
            Span::styled("Kingdom ", Style::default().fg(HEADER_MUTED)),
            Span::styled(
                species.taxonomy.kingdom.as_deref().unwrap_or("Unknown"),
                Style::default().fg(HEADER_TEXT).bold(),
            ),
            Span::styled("  Rank ", Style::default().fg(HEADER_MUTED)),
            Span::styled(rank_label, Style::default().fg(HEADER_TEXT)),
        ]),
    ];
    frame.render_widget(Paragraph::new(left_lines), cols[0]);

    let mut right_spans = vec![
        badge(
            if has_offline_search { "INDEX" } else { "LIVE" },
            Color::Black,
            ACCENT_MINT,
        ),
        Span::raw(" "),
        badge("REAL DATA", Color::Black, ACCENT_SKY),
    ];
    if is_favorite {
        right_spans.push(Span::raw(" "));
        right_spans.push(badge("SAVED", Color::Black, ACCENT_YELLOW));
    }
    let right_lines = vec![
        Line::from(vec![
            Span::styled("Catalog ", Style::default().fg(HEADER_MUTED)),
            Span::styled(catalog_id, Style::default().fg(HEADER_TEXT).bold()),
        ]),
        Line::from(vec![
            Span::styled("Cache ", Style::default().fg(HEADER_MUTED)),
            Span::styled("ready", Style::default().fg(HEADER_TEXT)),
            Span::styled("  r ", Style::default().fg(HEADER_MUTED)),
            Span::styled("ref", Style::default().fg(HEADER_TEXT)),
        ]),
        Line::from(right_spans),
    ];
    frame.render_widget(
        Paragraph::new(right_lines).alignment(Alignment::Right),
        cols[1],
    );
}

fn render_status_bar(
    frame: &mut Frame,
    area: Rect,
    status: &StatusBanner,
    loading: bool,
    spinner_frame: usize,
    is_favorite: bool,
    has_offline_search: bool,
) {
    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(HEADER_MUTED))
        .style(Style::default().bg(SHELL_EDGE));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).split(inner);
    let tone_color = match status.tone {
        StatusTone::Info => Color::Cyan,
        StatusTone::Success => Color::Green,
        StatusTone::Warning => Color::Yellow,
        StatusTone::Error => Color::Rgb(176, 138, 104),
    };

    let mut status_spans = vec![
        badge(
            match status.tone {
                StatusTone::Info => "LOG",
                StatusTone::Success => "READY",
                StatusTone::Warning => "NOTE",
                StatusTone::Error => "ALERT",
            },
            Color::Black,
            tone_color,
        ),
        Span::raw(" "),
        Span::styled(&status.message, Style::default().fg(SHELL_PANEL)),
    ];

    if loading {
        status_spans.push(Span::raw("  "));
        status_spans.push(Span::styled(
            spinner_char(spinner_frame),
            Style::default().fg(ACCENT_YELLOW),
        ));
        status_spans.push(Span::styled(
            " syncing",
            Style::default().fg(ACCENT_MINT).italic(),
        ));
    }

    frame.render_widget(Paragraph::new(Line::from(status_spans)), rows[0]);

    let collection_badge = if is_favorite {
        badge("SAVED", Color::Black, ACCENT_YELLOW)
    } else {
        badge("UNSAVED", Color::Black, Color::Rgb(112, 118, 122))
    };
    let search_badge = if has_offline_search {
        badge("INDEX READY", Color::Black, ACCENT_MINT)
    } else {
        badge("LIVE SEARCH", Color::Black, ACCENT_YELLOW)
    };

    let controls = Line::from(vec![
        collection_badge,
        Span::raw(" "),
        search_badge,
        Span::raw("  "),
        Span::styled("t ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("mode", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("↑↓ ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("move", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("← ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("up", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("→ ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("open", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("/ ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("search", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("r ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("ref", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("f ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("save", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("? ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("help", Style::default().fg(Color::Rgb(224, 228, 231))),
        Span::raw("  "),
        Span::styled("q ", Style::default().fg(ACCENT_YELLOW)),
        Span::styled("quit", Style::default().fg(Color::Rgb(224, 228, 231))),
    ]);
    frame.render_widget(Paragraph::new(controls), rows[1]);
}

fn render_search_overlay(
    frame: &mut Frame,
    area: Rect,
    query: &str,
    suggestions: Option<&[SearchSuggestion]>,
    selected: usize,
    has_offline_search: bool,
) {
    let suggestion_count = suggestions.map(|s| s.len()).unwrap_or(0);
    let popup_width = 68.min(area.width.saturating_sub(4));
    let popup_height = (6 + suggestion_count.min(10) as u16).min(area.height.saturating_sub(4));
    let popup_area = Rect::new(
        area.x + (area.width.saturating_sub(popup_width)) / 2,
        area.y + (area.height.saturating_sub(popup_height)) / 3,
        popup_width,
        popup_height,
    );

    frame.render_widget(Clear, popup_area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(SHELL_EDGE))
        .title(" Search Dex ")
        .title_style(Style::default().fg(Color::Black).bg(ACCENT_YELLOW).bold())
        .style(Style::default().bg(SHELL_PANEL));

    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(2),
        Constraint::Min(1),
    ])
    .split(inner);

    let input = Paragraph::new(Line::from(vec![
        Span::styled("name> ", Style::default().fg(SHELL_EDGE)),
        Span::styled(query, Style::default().fg(HEADER_TEXT).bold()),
        Span::styled("│", Style::default().fg(SHELL_EDGE)),
    ]));
    frame.render_widget(input, chunks[0]);

    let hint = if has_offline_search {
        "Type 2+ letters for indexed suggestions. Enter opens the highlighted or typed name."
    } else {
        "Offline taxonomy index is missing. Type a full name and press Enter to search live."
    };
    frame.render_widget(
        Paragraph::new(Span::styled(
            hint,
            Style::default().fg(HEADER_MUTED).italic(),
        ))
        .wrap(Wrap { trim: true }),
        chunks[1],
    );

    match suggestions {
        Some(sugs) if !sugs.is_empty() => {
            let visible = chunks[2].height as usize;
            let scroll = if selected >= visible {
                selected - visible + 1
            } else {
                0
            };

            let lines: Vec<Line> = sugs
                .iter()
                .enumerate()
                .skip(scroll)
                .take(visible)
                .map(|(i, s)| {
                    let is_selected = i == selected;
                    let display_name = s.canonical_name.as_ref().unwrap_or(&s.name);
                    let rank_short = match s.rank.to_uppercase().as_str() {
                        "SPECIES" => "sp",
                        "GENUS" => "gen",
                        "FAMILY" => "fam",
                        "ORDER" => "ord",
                        "CLASS" => "cls",
                        "PHYLUM" => "phy",
                        "KINGDOM" => "kgd",
                        _ => &s.rank,
                    };

                    let style = if is_selected {
                        Style::default().fg(Color::Black).bg(ACCENT_SKY)
                    } else {
                        Style::default().fg(HEADER_TEXT)
                    };

                    let marker = if is_selected { "▶ " } else { "  " };
                    let name_style = if s.canonical_name.is_some() {
                        style
                    } else {
                        scientific_name_style(&s.rank, style)
                    };

                    Line::from(vec![
                        Span::styled(marker, Style::default().fg(ACCENT_YELLOW)),
                        Span::styled(display_name, name_style),
                        Span::styled(
                            format!(" [{}]", rank_short),
                            if is_selected {
                                style
                            } else {
                                Style::default().fg(HEADER_MUTED)
                            },
                        ),
                    ])
                })
                .collect();

            frame.render_widget(Paragraph::new(lines), chunks[2]);
        }
        Some(_) => {
            frame.render_widget(
                Paragraph::new(Span::styled(
                    "No indexed match yet. Press Enter to open the typed name directly.",
                    Style::default().fg(HEADER_MUTED).italic(),
                ))
                .alignment(Alignment::Center)
                .wrap(Wrap { trim: true }),
                chunks[2],
            );
        }
        None => {
            frame.render_widget(
                Paragraph::new(Span::styled(
                    "Start typing to search the local dex.",
                    Style::default().fg(HEADER_MUTED).italic(),
                ))
                .alignment(Alignment::Center),
                chunks[2],
            );
        }
    }
}

fn render_wide_layout(frame: &mut Frame, area: Rect, state: &mut RenderState<'_>) {
    let chunks = Layout::horizontal([
        Constraint::Percentage(30),
        Constraint::Percentage(32),
        Constraint::Percentage(38),
    ])
    .split(area);

    render_image_panel(
        frame,
        chunks[0],
        state.species,
        state.image_state,
        state.map_state,
        state.is_favorite,
    );
    render_stats_panel(frame, chunks[1], state.species, state.is_favorite);
    render_taxonomy_panel(frame, chunks[2], state);
}

fn render_medium_layout(frame: &mut Frame, area: Rect, state: &mut RenderState<'_>) {
    let main_chunks =
        Layout::vertical([Constraint::Percentage(42), Constraint::Percentage(58)]).split(area);

    let top_chunks = Layout::horizontal([Constraint::Percentage(36), Constraint::Percentage(64)])
        .split(main_chunks[0]);

    render_image_panel(
        frame,
        top_chunks[0],
        state.species,
        state.image_state,
        state.map_state,
        state.is_favorite,
    );
    render_stats_panel(frame, top_chunks[1], state.species, state.is_favorite);
    render_taxonomy_panel(frame, main_chunks[1], state);
}

fn render_narrow_layout(frame: &mut Frame, area: Rect, state: &mut RenderState<'_>) {
    let chunks = Layout::vertical([
        Constraint::Length(15),
        Constraint::Length(10),
        Constraint::Min(8),
    ])
    .split(area);

    render_image_panel(
        frame,
        chunks[0],
        state.species,
        state.image_state,
        state.map_state,
        state.is_favorite,
    );
    render_stats_panel(frame, chunks[1], state.species, state.is_favorite);
    render_taxonomy_panel(frame, chunks[2], state);
}

fn render_image_panel(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    image_state: &mut Option<PortraitImageState>,
    map_state: &mut Option<MapImageState>,
    is_favorite: bool,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(SCREEN_EDGE))
        .style(Style::default().bg(DATA_BG))
        .title(" Portrait ")
        .title_style(Style::default().fg(Color::Black).bg(ACCENT_MINT).bold());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let show_range = inner.height >= 10;
    if show_range {
        let map_height = preferred_range_panel_height(inner.height);
        let chunks =
            Layout::vertical([Constraint::Min(0), Constraint::Length(map_height)]).split(inner);
        render_portrait_view(frame, chunks[0], species, image_state, is_favorite);
        render_range_strip(frame, chunks[1], species, map_state);
    } else {
        render_portrait_view(frame, inner, species, image_state, is_favorite);
    }
}

fn render_portrait_view(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    image_state: &mut Option<PortraitImageState>,
    is_favorite: bool,
) {
    let kingdom_color = get_kingdom_color(&species.taxonomy.kingdom);
    let has_archived_art = preferred_image_info(species).is_some();

    if let Some(state) = image_state {
        let image_widget = StatefulImage::new().resize(Resize::Fit(None));
        frame.render_stateful_widget(image_widget, area, &mut state.protocol);
    } else {
        render_image_placeholder(
            frame,
            area,
            species,
            kingdom_color,
            is_favorite,
            has_archived_art,
        );
    }
}

fn render_image_placeholder(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    kingdom_color: Color,
    is_favorite: bool,
    has_archived_art: bool,
) {
    let mut lines = vec![
        Line::from(Span::styled(
            kingdom_label(&species.taxonomy.kingdom),
            Style::default().fg(kingdom_color).bold(),
        )),
        Line::from(""),
        Line::from(Span::styled(
            if has_archived_art {
                "PORTRAIT READY"
            } else {
                "NO PORTRAIT"
            },
            Style::default().fg(SHELL_PANEL).bold(),
        )),
        Line::from(Span::styled(
            if has_archived_art {
                "terminal cannot render raster art"
            } else {
                "cached data ready"
            },
            Style::default().fg(Color::Rgb(224, 228, 231)),
        )),
        Line::from(""),
        Line::from(Span::styled(
            if is_favorite {
                "saved"
            } else {
                "f saves this entry"
            },
            Style::default().fg(ACCENT_YELLOW),
        )),
    ];

    if area.height >= 8 {
        let fallback_name = primary_common_name(species).unwrap_or(&species.scientific_name);
        let fallback_style = if primary_common_name(species).is_some() {
            Style::default().fg(HEADER_MUTED)
        } else {
            scientific_name_style(&species.rank, Style::default().fg(HEADER_MUTED))
        };
        lines.push(Line::from(Span::styled(
            fallback_name.to_string(),
            fallback_style,
        )));
    }

    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_range_strip(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    map_state: &mut Option<MapImageState>,
) {
    let title = if !species.distribution.continents.is_empty() {
        format!(
            " Range · {} ",
            trim_for_line(&species.distribution.continents.join(", "), 20)
        )
    } else {
        " Range ".to_string()
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(SCREEN_EDGE))
        .style(Style::default().bg(PANEL_BG))
        .title(title)
        .title_style(Style::default().fg(Color::Black).bg(ACCENT_MINT).bold());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width < 10 || inner.height == 0 {
        return;
    }

    if let Some(state) = map_state {
        if inner.width >= 10 && inner.height >= 2 {
            let map_widget = StatefulImage::new().resize(Resize::Fit(None));
            let protocol = state.protocol_for_area(inner);
            frame.render_stateful_widget(map_widget, inner, protocol);
            return;
        }
    }

    if inner.width >= 16 && inner.height >= 3 {
        render_ascii_range_content(frame, inner, species, false);
    } else {
        frame.render_widget(
            Paragraph::new(Line::from(range_summary_spans(species))).wrap(Wrap { trim: true }),
            inner,
        );
    }
}

fn render_stats_panel(frame: &mut Frame, area: Rect, species: &UnifiedSpecies, is_favorite: bool) {
    let is_species = is_species_rank(&species.rank);
    let title = if is_species {
        " Dex Stats "
    } else {
        " Taxon Notes "
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(DATA_EDGE))
        .title(title)
        .title_style(Style::default().fg(Color::Black).bg(DATA_EDGE).bold())
        .style(Style::default().bg(PANEL_BG));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width < 24 {
        return;
    }

    if !is_species {
        render_taxon_summary_panel(frame, inner, species);
        return;
    }

    if inner.height < 6 {
        render_species_taxonomy_summary(frame, inner, species, is_favorite);
        return;
    }

    let summary_height = if inner.height >= 18 {
        5
    } else if inner.height >= 14 {
        4
    } else {
        3
    };
    let chunks =
        Layout::vertical([Constraint::Length(summary_height), Constraint::Min(3)]).split(inner);

    render_species_taxonomy_summary(frame, chunks[0], species, is_favorite);
    render_species_stat_section(frame, chunks[1], species);
}

fn render_species_stat_section(frame: &mut Frame, area: Rect, species: &UnifiedSpecies) {
    if area.height == 0 {
        return;
    }

    let meters = stat_meters(species);
    let expanded_meters = area.height >= 7 && area.width >= 24;
    let meter_height = if expanded_meters { 2 } else { 1 };
    let footer_reserve = if expanded_meters { 3 } else { 1 };
    let max_meter_rows = area.height.saturating_sub(footer_reserve);
    let height_limited_count = (max_meter_rows / meter_height).max(1) as usize;
    let preferred_count = if area.height >= 10 {
        5
    } else if area.height >= 8 {
        4
    } else if area.height >= 6 {
        3
    } else {
        2
    };
    let bar_count = preferred_count.min(height_limited_count).min(meters.len());

    let mut constraints = vec![Constraint::Length(meter_height); bar_count];
    let show_footer = area.height > bar_count as u16 * meter_height;
    if show_footer {
        constraints.push(Constraint::Min(1));
    }
    let chunks = Layout::vertical(constraints).split(area);

    for (chunk, meter) in chunks.iter().take(bar_count).zip(meters.iter()) {
        render_stat_meter(frame, *chunk, meter);
    }

    if show_footer {
        render_stats_footer(frame, chunks[bar_count], species);
    }
}

fn render_stat_meter(frame: &mut Frame, area: Rect, meter: &StatMeter) {
    if area.height >= 2 {
        let rows = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).split(area);
        let header_cols =
            Layout::horizontal([Constraint::Min(10), Constraint::Length(14)]).split(rows[0]);

        frame.render_widget(
            Paragraph::new(Span::styled(
                meter.label,
                Style::default().fg(HEADER_MUTED).bold(),
            )),
            header_cols[0],
        );
        frame.render_widget(
            Paragraph::new(Span::styled(
                trim_for_line(&meter.value, header_cols[1].width as usize),
                Style::default().fg(meter.color).bold(),
            ))
            .alignment(Alignment::Right),
            header_cols[1],
        );
        frame.render_widget(
            Paragraph::new(stat_meter_bar(area.width as usize, meter)),
            rows[1],
        );
        return;
    }

    let cols = Layout::horizontal([
        Constraint::Length(10),
        Constraint::Min(8),
        Constraint::Length(12),
    ])
    .split(area);

    frame.render_widget(
        Paragraph::new(Span::styled(
            format!("{: <9}", meter.label),
            Style::default().fg(HEADER_MUTED),
        )),
        cols[0],
    );
    frame.render_widget(
        Paragraph::new(stat_meter_bar(cols[1].width as usize, meter)),
        cols[1],
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            trim_for_line(&meter.value, cols[2].width as usize),
            Style::default().fg(SHELL_PANEL).bold(),
        ))
        .alignment(Alignment::Right),
        cols[2],
    );
}

fn preferred_range_panel_height(total_height: u16) -> u16 {
    let minimum = if total_height >= 20 { 8 } else { 5 };
    let maximum = total_height.saturating_sub(6).max(minimum);
    ((total_height as f32 * 0.34).round() as u16).clamp(minimum, maximum)
}

fn render_stats_footer(frame: &mut Frame, area: Rect, species: &UnifiedSpecies) {
    let mut lines = vec![
        Line::from(reproduction_summary_spans(species)),
        Line::from(compact_genome_spans(species)),
        Line::from(range_summary_spans(species)),
    ];

    if area.height >= 4 {
        lines.push(Line::from(source_badges(species)));
    }

    if area.height >= 5 {
        if let Some(notes) = field_notes_text(species) {
            lines.push(Line::from(Span::styled(
                trim_for_line(&notes, area.width.saturating_mul(2) as usize),
                Style::default().fg(Color::Rgb(214, 218, 222)),
            )));
        }
    }

    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), area);
}

fn render_taxon_summary_panel(frame: &mut Frame, area: Rect, species: &UnifiedSpecies) {
    let rank_label = display_rank(&species.rank);
    let child_rank = child_rank_for(&species.rank).map(display_rank);
    let common_name = primary_common_name(species);

    let mut lines = vec![Line::from(Span::styled(
        trim_for_line(&species.scientific_name, area.width as usize),
        scientific_name_style(&species.rank, Style::default().fg(SHELL_PANEL).bold()),
    ))];

    if let Some(common_name) = common_name {
        if area.height >= 2 {
            lines.push(Line::from(Span::styled(
                trim_for_line(common_name, area.width as usize),
                Style::default().fg(ACCENT_MINT),
            )));
        }
    }

    if area.height >= 2 {
        let mut badges = vec![badge(&rank_label, Color::Black, ACCENT_SKY)];
        if let Some(next_rank) = child_rank.as_deref() {
            badges.push(Span::raw(" "));
            badges.push(badge(next_rank, Color::Black, ACCENT_MINT));
        }
        lines.push(Line::from(badges));
    }

    if area.height >= 3 {
        lines.push(Line::from(compact_taxonomy_spans(
            "Kingdom",
            species.taxonomy.kingdom.as_deref(),
            "Phylum",
            species.taxonomy.phylum.as_deref(),
        )));
    }
    if area.height >= 4 {
        lines.push(Line::from(compact_taxonomy_spans(
            "Class",
            species.taxonomy.class.as_deref(),
            "Order",
            species.taxonomy.order.as_deref(),
        )));
    }
    if area.height >= 5 {
        lines.push(Line::from(compact_taxonomy_spans(
            "Family",
            species.taxonomy.family.as_deref(),
            "Genus",
            species.taxonomy.genus.as_deref(),
        )));
    }
    if area.height >= 6 {
        let guidance = if let Some(next_rank) = child_rank.as_deref() {
            format!("Browse {next_rank} entries with l. Open one for full organism stats.")
        } else {
            "Browse the lineage to move into concrete organism entries.".to_string()
        };
        lines.push(Line::from(Span::styled(
            trim_for_line(&guidance, area.width.saturating_mul(2) as usize),
            Style::default().fg(Color::Rgb(214, 218, 222)),
        )));
    }
    if area.height >= 7 {
        lines.push(Line::from(range_summary_spans(species)));
    }
    if area.height >= 8 {
        lines.push(Line::from(source_badges(species)));
    }

    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), area);
}

fn render_taxonomy_panel(frame: &mut Frame, area: Rect, state: &RenderState<'_>) {
    let mode_title = match state.navigator_focus {
        NavigatorFocus::SpeciesList => " Species Navigator ",
        NavigatorFocus::Taxonomy => " Taxonomy Browser ",
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(DATA_EDGE))
        .style(Style::default().bg(PANEL_BG))
        .title(mode_title)
        .title_style(Style::default().fg(Color::Black).bg(DATA_EDGE).bold());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 {
        render_navigation_lists(frame, inner, state);
        return;
    }
    render_navigation_lists(frame, inner, state);
}

fn render_navigation_lists(frame: &mut Frame, area: Rect, state: &RenderState<'_>) {
    let (entries, title, selected, show_rank) = match state.navigator_focus {
        NavigatorFocus::SpeciesList => (
            state.species_list_entries,
            "Species List",
            state.species_list_index,
            false,
        ),
        NavigatorFocus::Taxonomy => (
            state.browser_entries,
            state.browser_title,
            state.browser_index,
            true,
        ),
    };
    render_browser_list(frame, area, entries, title, selected, true, show_rank);
}

fn render_species_taxonomy_summary(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    is_favorite: bool,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let inner = area;
    let kingdom_color = get_kingdom_color(&species.taxonomy.kingdom);
    let rank_label = display_rank(&species.rank);
    let lineage = species
        .taxonomy
        .build_display_lineage(&species.scientific_name, &species.rank)
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>()
        .join(" > ");

    let mut badges = vec![
        badge(
            species.taxonomy.kingdom.as_deref().unwrap_or("Unknown"),
            Color::White,
            kingdom_color,
        ),
        Span::raw(" "),
        badge(&rank_label, Color::Black, ACCENT_SKY),
    ];

    if is_favorite {
        badges.push(Span::raw(" "));
        badges.push(badge("SAVED", Color::Black, ACCENT_YELLOW));
    }

    if let Some(status) = species
        .iucn_status
        .as_deref()
        .filter(|status| matches!(*status, "LC" | "NT" | "VU" | "EN" | "CR" | "EW" | "EX"))
    {
        badges.push(Span::raw(" "));
        badges.push(badge(status, Color::White, conservation_color(status)));
    }

    let mut lines = vec![Line::from(badges)];
    if inner.height >= 2 {
        lines.push(Line::from(vec![
            Span::styled("History ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                trim_for_line(&lineage, inner.width.saturating_sub(8) as usize),
                Style::default().fg(HEADER_MUTED),
            ),
        ]));
    }
    if inner.height >= 3 {
        lines.push(Line::from(compact_taxonomy_spans(
            "Kingdom",
            species.taxonomy.kingdom.as_deref(),
            "Phylum",
            species.taxonomy.phylum.as_deref(),
        )));
    }
    if inner.height >= 4 {
        lines.push(Line::from(compact_taxonomy_spans(
            "Class",
            species.taxonomy.class.as_deref(),
            "Order",
            species.taxonomy.order.as_deref(),
        )));
    }
    if inner.height >= 5 {
        lines.push(Line::from(compact_taxonomy_spans(
            "Family",
            species.taxonomy.family.as_deref(),
            "Genus",
            species.taxonomy.genus.as_deref(),
        )));
    }
    if inner.height >= 6 {
        lines.push(Line::from(source_badges(species)));
    }

    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
}

fn render_browser_list(
    frame: &mut Frame,
    area: Rect,
    entries: &[SiblingTaxon],
    browser_title: &str,
    selected: usize,
    focused: bool,
    show_rank: bool,
) {
    let accent = if focused { ACCENT_YELLOW } else { ACCENT_SKY };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(accent))
        .style(Style::default().bg(DATA_BG))
        .title(format!(" {} · {} ", browser_title, entries.len()))
        .title_style(Style::default().fg(Color::Black).bg(accent).bold());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    if entries.is_empty() {
        let msg = Paragraph::new("No cached entries here yet")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        frame.render_widget(msg, inner);
        return;
    }

    let visible_height = inner.height as usize;
    let total = entries.len();

    let scroll = if selected >= visible_height {
        selected - visible_height + 1
    } else {
        0
    }
    .min(total.saturating_sub(visible_height));

    let lines: Vec<Line> = entries
        .iter()
        .enumerate()
        .skip(scroll)
        .take(visible_height)
        .map(|(i, entry)| {
            let is_selected = i == selected;
            let rank_color = get_rank_color(&entry.rank);
            let style = if is_selected && focused {
                Style::default().fg(Color::Black).bg(rank_color).bold()
            } else if is_selected {
                Style::default().fg(SHELL_PANEL).bg(PANEL_BORDER).bold()
            } else {
                Style::default().fg(Color::White)
            };
            let selector = if is_selected { "▶ " } else { "  " };
            let rank_suffix = if show_rank {
                format!(" [{}]", display_rank(&entry.rank))
            } else {
                String::new()
            };
            let max_name_chars = inner
                .width
                .saturating_sub(rank_suffix.chars().count() as u16 + 2)
                as usize;
            let display_name = trim_for_line(&entry.name, max_name_chars.max(1));

            let mut spans = vec![
                Span::styled(
                    selector,
                    Style::default().fg(if focused { ACCENT_YELLOW } else { HEADER_MUTED }),
                ),
                Span::styled(display_name, scientific_name_style(&entry.rank, style)),
            ];
            if show_rank {
                spans.push(Span::styled(
                    rank_suffix,
                    if is_selected && focused {
                        style
                    } else {
                        Style::default().fg(rank_color)
                    },
                ));
            }

            Line::from(spans)
        })
        .collect();

    frame.render_widget(Paragraph::new(lines), inner);

    if total > visible_height {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        let mut scrollbar_state = ScrollbarState::new(total).position(scroll);

        frame.render_stateful_widget(
            scrollbar,
            area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut scrollbar_state,
        );
    }
}

#[derive(Clone)]
struct StatMeter {
    label: &'static str,
    value: String,
    ratio: f64,
    color: Color,
}

fn render_ascii_range_content(
    frame: &mut Frame,
    area: Rect,
    species: &UnifiedSpecies,
    show_legend: bool,
) {
    if area.width < 8 || area.height == 0 {
        return;
    }

    let sections = if show_legend && area.height >= 2 {
        Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(area)
    } else {
        Layout::vertical([Constraint::Min(1)]).split(area)
    };
    let atlas_area = sections[0];
    let bbox = species.distribution.bounding_box.as_ref().map(|bbox| {
        (
            bbox.min_latitude,
            bbox.max_latitude,
            bbox.min_longitude,
            bbox.max_longitude,
        )
    });
    let atlas = crate::world_map::generate_ascii_range_map(
        atlas_area.width as usize,
        atlas_area.height as usize,
        bbox,
        &species.distribution.continents,
    );
    let atlas_lines = atlas
        .iter()
        .map(|row| styled_ascii_atlas_line(row))
        .collect::<Vec<_>>();

    frame.render_widget(Paragraph::new(atlas_lines), atlas_area);

    if sections.len() > 1 {
        let legend = Line::from(vec![
            Span::styled(
                "#",
                Style::default()
                    .fg(Color::Rgb(255, 221, 110))
                    .bg(Color::Rgb(24, 38, 58))
                    .bold(),
            ),
            Span::styled(" range  ", Style::default().fg(Color::Rgb(224, 228, 231))),
            Span::styled(
                ".",
                Style::default()
                    .fg(Color::Rgb(176, 205, 145))
                    .bg(Color::Rgb(36, 58, 40)),
            ),
            Span::styled(" land  ", Style::default().fg(Color::Rgb(224, 228, 231))),
            Span::styled(
                "~",
                Style::default()
                    .fg(Color::Rgb(128, 182, 238))
                    .bg(Color::Rgb(18, 38, 64)),
            ),
            Span::styled(" ocean", Style::default().fg(Color::Rgb(224, 228, 231))),
        ]);
        frame.render_widget(Paragraph::new(legend), sections[1]);
    }
}

fn stat_meters(species: &UnifiedSpecies) -> Vec<StatMeter> {
    let lifespan = species.life_history.lifespan_years;
    let size = primary_size_meters(species);
    let mass = species.life_history.mass_kilograms;
    let genome_bp = species.genome.genome_size_bp.map(|bp| bp as f64);
    let conservation = primary_conservation_status(species);

    vec![
        StatMeter {
            label: "Lifespan",
            value: lifespan
                .map(format_lifespan_compact)
                .unwrap_or_else(|| "unlogged".to_string()),
            ratio: log_scaled_ratio(lifespan, 250.0),
            color: ACCENT_MINT,
        },
        StatMeter {
            label: primary_size_label(species),
            value: size
                .map(format_length_compact)
                .unwrap_or_else(|| "unlogged".to_string()),
            ratio: log_scaled_ratio(size, 35.0),
            color: ACCENT_SKY,
        },
        StatMeter {
            label: "Mass",
            value: mass
                .map(format_mass_compact)
                .unwrap_or_else(|| "unlogged".to_string()),
            ratio: log_scaled_ratio(mass, 100_000.0),
            color: Color::Rgb(214, 193, 142),
        },
        StatMeter {
            label: "Genome",
            value: genome_bp
                .map(|bp| format!("{:.2} Gb", bp / 1e9))
                .unwrap_or_else(|| "unlogged".to_string()),
            ratio: log_scaled_ratio(genome_bp, 150_000_000_000.0),
            color: Color::Rgb(161, 150, 194),
        },
        StatMeter {
            label: "Threat",
            value: conservation
                .map(display_conservation_status)
                .unwrap_or_else(|| "unlogged".to_string()),
            ratio: conservation.map(conservation_ratio).unwrap_or(0.0),
            color: conservation
                .map(conservation_color)
                .unwrap_or(Color::Rgb(142, 150, 158)),
        },
    ]
}

fn stat_meter_bar(width: usize, meter: &StatMeter) -> Line<'static> {
    if width == 0 {
        return Line::default();
    }

    let exact = meter.ratio.clamp(0.0, 1.0) * width as f64;
    let full = exact.floor() as usize;
    let partial_step = ((exact - full as f64) * 8.0).round() as usize;
    let partial = match partial_step {
        1 => Some('▏'),
        2 => Some('▎'),
        3 => Some('▍'),
        4 => Some('▌'),
        5 => Some('▋'),
        6 => Some('▊'),
        7 => Some('▉'),
        8 => Some('█'),
        _ => None,
    };

    let mut filled = "█".repeat(full.min(width));
    if full < width {
        if let Some(partial) = partial {
            filled.push(partial);
        }
    }

    let used = filled.chars().count();
    let empty = width.saturating_sub(used);
    let empty_char = if used == 0 { '·' } else { '─' };

    let mut spans = Vec::new();
    if !filled.is_empty() {
        spans.push(Span::styled(
            filled,
            Style::default().fg(meter.color).bold(),
        ));
    }
    if empty > 0 {
        spans.push(Span::styled(
            empty_char.to_string().repeat(empty),
            Style::default().fg(PANEL_BORDER),
        ));
    }

    Line::from(spans)
}

fn log_scaled_ratio(value: Option<f64>, max: f64) -> f64 {
    let Some(value) = value else {
        return 0.0;
    };
    if value <= 0.0 || max <= 0.0 {
        return 0.0;
    }

    ((value + 1.0).ln() / (max + 1.0).ln()).clamp(0.0, 1.0)
}

fn spinner_char(spinner_frame: usize) -> &'static str {
    const SPINNER: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    SPINNER[spinner_frame % SPINNER.len()]
}

fn badge<'a>(label: &'a str, fg: Color, bg: Color) -> Span<'a> {
    Span::styled(
        format!(" {} ", label),
        Style::default().fg(fg).bg(bg).bold(),
    )
}

fn preferred_image_info(species: &UnifiedSpecies) -> Option<&ImageInfo> {
    species
        .images
        .iter()
        .find(|img| img.source == "iNaturalist")
        .or_else(|| species.images.iter().find(|img| img.source == "Wikipedia"))
        .or_else(|| species.images.iter().find(|img| img.source == "Wikidata"))
        .or_else(|| species.images.first())
}

fn styled_ascii_atlas_line(row: &str) -> Line<'static> {
    let mut spans = Vec::new();
    let mut current_style = None;
    let mut buffer = String::new();

    for ch in row.chars() {
        let style = ascii_atlas_style(ch);
        if current_style == Some(style) {
            buffer.push(ch);
        } else {
            if !buffer.is_empty() {
                spans.push(Span::styled(
                    std::mem::take(&mut buffer),
                    current_style.unwrap(),
                ));
            }
            buffer.push(ch);
            current_style = Some(style);
        }
    }

    if !buffer.is_empty() {
        spans.push(Span::styled(buffer, current_style.unwrap()));
    }

    Line::from(spans)
}

fn ascii_atlas_style(ch: char) -> Style {
    match ch {
        '#' => Style::default()
            .fg(Color::Rgb(255, 221, 110))
            .bg(Color::Rgb(24, 38, 58))
            .bold(),
        '.' => Style::default()
            .fg(Color::Rgb(176, 205, 145))
            .bg(Color::Rgb(36, 58, 40)),
        '~' => Style::default()
            .fg(Color::Rgb(128, 182, 238))
            .bg(Color::Rgb(18, 38, 64)),
        _ => Style::default().fg(Color::DarkGray),
    }
}

fn kingdom_label(kingdom: &Option<String>) -> &'static str {
    match kingdom.as_deref() {
        Some("Animalia") => "ANIMALIA",
        Some("Plantae") => "PLANTAE",
        Some("Fungi") => "FUNGI",
        Some("Bacteria") => "BACTERIA",
        Some("Archaea") => "ARCHAEA",
        Some("Protista") => "PROTISTA",
        Some("Chromista") => "CHROMISTA",
        _ => "SPECIMEN",
    }
}

fn display_rank(rank: &str) -> String {
    let trimmed = rank.trim();
    if trimmed.is_empty() {
        return "Species".to_string();
    }

    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return "Species".to_string();
    };

    let mut normalized = String::with_capacity(trimmed.len());
    normalized.extend(first.to_uppercase());
    normalized.push_str(&chars.as_str().to_lowercase());
    normalized
}

fn conservation_color(status: &str) -> Color {
    match normalized_conservation_code(status) {
        Some("LC") => Color::Green,
        Some("NT") => Color::LightGreen,
        Some("VU") => Color::Yellow,
        Some("EN") => Color::Rgb(255, 165, 0),
        Some("CR") => Color::Rgb(176, 138, 104),
        Some("EW") | Some("EX") => Color::DarkGray,
        _ => Color::Gray,
    }
}

fn primary_conservation_status(species: &UnifiedSpecies) -> Option<&str> {
    species
        .iucn_status
        .as_deref()
        .or(species.conservation_status.as_deref())
}

fn normalized_conservation_code(status: &str) -> Option<&'static str> {
    let normalized = status.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "lc" | "least concern" => Some("LC"),
        "nt" | "near threatened" => Some("NT"),
        "vu" | "vulnerable" => Some("VU"),
        "en" | "endangered" => Some("EN"),
        "cr" | "critically endangered" => Some("CR"),
        "ew" | "extinct in the wild" => Some("EW"),
        "ex" | "extinct" => Some("EX"),
        _ => None,
    }
}

fn display_conservation_status(status: &str) -> String {
    match normalized_conservation_code(status) {
        Some("LC") => "Least concern".to_string(),
        Some("NT") => "Near threat".to_string(),
        Some("VU") => "Vulnerable".to_string(),
        Some("EN") => "Endangered".to_string(),
        Some("CR") => "Critical".to_string(),
        Some("EW") => "Wild extinct".to_string(),
        Some("EX") => "Extinct".to_string(),
        _ => trim_for_line(status, 12),
    }
}

fn conservation_ratio(status: &str) -> f64 {
    match normalized_conservation_code(status) {
        Some("LC") => 0.18,
        Some("NT") => 0.34,
        Some("VU") => 0.52,
        Some("EN") => 0.72,
        Some("CR") => 0.9,
        Some("EW") | Some("EX") => 1.0,
        _ => 0.0,
    }
}

fn source_badges(species: &UnifiedSpecies) -> Vec<Span<'static>> {
    let mut spans = Vec::new();

    if species.ids.ncbi_tax_id.is_some() {
        spans.push(badge("NCBI", Color::Black, Color::Rgb(135, 206, 235)));
    }
    if species.ids.gbif_key.is_some() {
        if !spans.is_empty() {
            spans.push(Span::raw(" "));
        }
        spans.push(badge("GBIF", Color::Black, Color::Rgb(119, 221, 119)));
    }
    if species.ids.inat_id.is_some() {
        if !spans.is_empty() {
            spans.push(Span::raw(" "));
        }
        spans.push(badge("INAT", Color::Black, Color::Rgb(255, 218, 121)));
    }
    if species.wikipedia_url.is_some() {
        if !spans.is_empty() {
            spans.push(Span::raw(" "));
        }
        spans.push(badge("WIKI", Color::White, Color::Rgb(90, 98, 112)));
    }
    if species.ids.ensembl_id.is_some() || species.genome.assembly_accession.is_some() {
        if !spans.is_empty() {
            spans.push(Span::raw(" "));
        }
        spans.push(badge("GENOME", Color::White, Color::Rgb(107, 91, 149)));
    }

    if spans.is_empty() {
        spans.push(Span::styled(
            "Sources still syncing",
            Style::default().fg(Color::DarkGray).italic(),
        ));
    }

    spans
}

fn trim_for_line(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let cleaned = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let char_count = cleaned.chars().count();
    if char_count <= max_chars {
        return cleaned;
    }

    let keep = max_chars.saturating_sub(1);
    let mut trimmed = cleaned.chars().take(keep).collect::<String>();
    trimmed.push('…');
    trimmed
}

fn field_notes_text(species: &UnifiedSpecies) -> Option<String> {
    species
        .description
        .as_deref()
        .or(species.wikipedia_extract.as_deref())
        .map(|text| trim_for_line(text, 320))
}

fn primary_size_label(species: &UnifiedSpecies) -> &'static str {
    if species.life_history.length_meters.is_some() {
        "Length"
    } else if species.life_history.height_meters.is_some() {
        "Height"
    } else {
        "Size"
    }
}

fn primary_size_meters(species: &UnifiedSpecies) -> Option<f64> {
    species
        .life_history
        .length_meters
        .or(species.life_history.height_meters)
}

fn reproduction_summary_spans(species: &UnifiedSpecies) -> Vec<Span<'static>> {
    let summary = if species.life_history.reproduction_modes.is_empty() {
        "still being logged".to_string()
    } else {
        trim_for_line(&species.life_history.reproduction_modes.join(" · "), 36)
    };

    vec![
        Span::styled("Repro ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            summary,
            if species.life_history.reproduction_modes.is_empty() {
                Style::default().fg(Color::Rgb(224, 228, 231))
            } else {
                Style::default().fg(Color::White)
            },
        ),
    ]
}

fn compact_taxonomy_spans(
    left_label: &str,
    left_value: Option<&str>,
    right_label: &str,
    right_value: Option<&str>,
) -> Vec<Span<'static>> {
    vec![
        Span::styled(
            format!("{left_label} "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            left_value.unwrap_or("-").to_string(),
            scientific_name_style(left_label, Style::default().fg(Color::White)),
        ),
        Span::raw(" │ "),
        Span::styled(
            format!("{right_label} "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            right_value.unwrap_or("-").to_string(),
            scientific_name_style(right_label, Style::default().fg(Color::White)),
        ),
    ]
}

fn compact_genome_spans(species: &UnifiedSpecies) -> Vec<Span<'static>> {
    let genome = &species.genome;
    let genome_size = genome
        .genome_size_bp
        .map(|size| format!("{:.2} Gb", size as f64 / 1e9))
        .unwrap_or_else(|| "unlogged".to_string());
    let chromosomes = genome
        .chromosome_count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unlogged".to_string());
    let gc_or_mito = genome
        .gc_percent
        .map(|gc| format!("{gc:.1}%"))
        .or_else(|| {
            genome
                .mito_genome_size_bp
                .map(|mito| format!("{:.1} kb", mito as f64 / 1000.0))
        })
        .unwrap_or_else(|| "unlogged".to_string());
    let tail_label = if genome.gc_percent.is_some() {
        "GC"
    } else {
        "Mito"
    };

    vec![
        Span::styled("Genome ", Style::default().fg(Color::DarkGray)),
        Span::styled(genome_size, Style::default().fg(Color::Cyan)),
        Span::raw(" │ "),
        Span::styled("Chr ", Style::default().fg(Color::DarkGray)),
        Span::styled(chromosomes, Style::default().fg(Color::Magenta)),
        Span::raw(" │ "),
        Span::styled(
            format!("{tail_label} "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(gc_or_mito, Style::default().fg(Color::Green)),
    ]
}

fn format_lifespan_compact(years: f64) -> String {
    if years >= 1.0 {
        format!("{} yr", format_measurement_value(years))
    } else if years * 12.0 >= 1.0 {
        format!("{} mo", format_measurement_value(years * 12.0))
    } else {
        format!("{} d", format_measurement_value(years * 365.25))
    }
}

fn format_length_compact(meters: f64) -> String {
    if meters >= 1.0 {
        format!("{} m", format_measurement_value(meters))
    } else if meters >= 0.01 {
        format!("{} cm", format_measurement_value(meters * 100.0))
    } else {
        format!("{} mm", format_measurement_value(meters * 1_000.0))
    }
}

fn format_mass_compact(kilograms: f64) -> String {
    if kilograms >= 1_000.0 {
        format!("{} t", format_measurement_value(kilograms / 1_000.0))
    } else if kilograms >= 1.0 {
        format!("{} kg", format_measurement_value(kilograms))
    } else {
        format!("{} g", format_measurement_value(kilograms * 1_000.0))
    }
}

fn format_measurement_value(value: f64) -> String {
    if value >= 10.0 || (value.round() - value).abs() < 0.05 {
        format!("{:.0}", value)
    } else {
        format!("{:.1}", value)
    }
}

fn range_summary_spans(species: &UnifiedSpecies) -> Vec<Span<'static>> {
    if !species.distribution.continents.is_empty() {
        vec![
            Span::styled("Range ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                species.distribution.continents.join(", "),
                Style::default().fg(Color::White),
            ),
        ]
    } else if let Some(native_range) = &species.distribution.native_range {
        vec![
            Span::styled("Range ", Style::default().fg(Color::DarkGray)),
            Span::styled(native_range.clone(), Style::default().fg(Color::White)),
        ]
    } else {
        vec![
            Span::styled("Range ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "still being mapped",
                Style::default().fg(Color::Rgb(224, 228, 231)),
            ),
        ]
    }
}

fn get_rank_color(rank: &str) -> Color {
    match rank.to_lowercase().as_str() {
        "kingdom" => Color::Rgb(108, 168, 164),
        "phylum" => Color::Rgb(255, 179, 71),
        "class" => Color::Yellow,
        "order" => Color::Green,
        "family" => Color::Cyan,
        "genus" => Color::Magenta,
        "species" => Color::White,
        _ => Color::DarkGray,
    }
}

fn primary_common_name(species: &UnifiedSpecies) -> Option<&str> {
    species
        .common_names
        .iter()
        .find(|name| !name.eq_ignore_ascii_case(&species.scientific_name))
        .map(String::as_str)
}

fn scientific_name_style(rank: &str, style: Style) -> Style {
    if scientific_rank_uses_italics(rank) {
        style.add_modifier(Modifier::ITALIC)
    } else {
        style
    }
}

fn scientific_rank_uses_italics(rank: &str) -> bool {
    matches!(
        rank.trim().to_ascii_lowercase().as_str(),
        "genus"
            | "subgenus"
            | "species"
            | "subspecies"
            | "variety"
            | "subvariety"
            | "form"
            | "subform"
    )
}

fn render_help(frame: &mut Frame) {
    let area = frame.area();

    let popup_width = 55.min(area.width.saturating_sub(4));
    let popup_height = 22.min(area.height.saturating_sub(4));
    let popup_area = Rect::new(
        (area.width - popup_width) / 2,
        (area.height - popup_height) / 2,
        popup_width,
        popup_height,
    );

    frame.render_widget(Clear, area);
    frame.render_widget(
        Block::default().style(Style::default().bg(Color::Black)),
        area,
    );

    let help_text = vec![
        Line::from(Span::styled(
            "FieldDex Controls",
            Style::default().bold().fg(Color::Cyan),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("↑/↓ j/k   ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Move through the active navigator mode"),
        ]),
        Line::from(vec![
            Span::styled("t         ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Swap between A-Z species list and taxonomy"),
        ]),
        Line::from(vec![
            Span::styled("pause     ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("In the A-Z list, auto-load the highlighted species"),
        ]),
        Line::from(vec![
            Span::styled("→ l / Ent ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Open the selected child or species"),
        ]),
        Line::from(vec![
            Span::styled("← h       ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Move up one taxonomy level"),
        ]),
        Line::from(vec![
            Span::styled("g / G     ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Jump to top / bottom"),
        ]),
        Line::from(vec![
            Span::styled("/         ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Search the local index"),
        ]),
        Line::from(vec![
            Span::styled("r         ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Refresh from live sources"),
        ]),
        Line::from(vec![
            Span::styled("f         ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Save or remove the current entry"),
        ]),
        Line::from(vec![
            Span::styled("?         ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(vec![
            Span::styled("b         ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Jump into taxonomy at the kingdom level"),
        ]),
        Line::from(vec![
            Span::styled("q / Esc   ", Style::default().fg(ACCENT_YELLOW)),
            Span::raw("Quit / close help"),
        ]),
    ];

    let help = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Help ")
                .title_style(Style::default().fg(Color::Cyan).bold()),
        )
        .alignment(Alignment::Center);

    frame.render_widget(help, popup_area);
}

fn primary_catalog_id(species: &UnifiedSpecies) -> String {
    species
        .ids
        .ncbi_tax_id
        .map(|tax_id| format!("NCBI {tax_id}"))
        .or_else(|| species.ids.gbif_key.map(|key| format!("GBIF {key}")))
        .or_else(|| species.ids.inat_id.map(|id| format!("iNat {id}")))
        .unwrap_or_else(|| "LOCAL".to_string())
}
