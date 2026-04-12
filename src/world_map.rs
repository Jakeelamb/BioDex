//! Simple world map background generator
//!
//! Creates a basic world map background for compositing with GBIF occurrence tiles.
//! Uses simplified continent rectangles for fast generation and clean TUI display.

use image::{imageops::FilterType, DynamicImage, Rgba, RgbaImage};
use std::sync::OnceLock;

/// Ocean colors for the base map.
const OCEAN_DEEP_COLOR: Rgba<u8> = Rgba([24, 53, 92, 255]);
const OCEAN_COLOR: Rgba<u8> = Rgba([56, 92, 138, 255]);
const OCEAN_SHALLOW_COLOR: Rgba<u8> = Rgba([92, 148, 204, 255]);
/// Subtle grid lines so open water still reads as "map" rather than blank background.
const OCEAN_GRID_COLOR: Rgba<u8> = Rgba([78, 122, 176, 255]);
/// Land color with enough separation from both the ocean and the yellow occurrence dots.
const LAND_COLOR: Rgba<u8> = Rgba([106, 142, 102, 255]);
/// Thin coastline accent to make continent edges readable at TUI sizes.
const COASTLINE_COLOR: Rgba<u8> = Rgba([188, 214, 165, 255]);

/// Size of the map tile (GBIF uses @2x = 512x512)
const TILE_SIZE: u32 = 512;
const TUI_MAP_ASPECT_WIDTH: u32 = 2;
const TUI_MAP_ASPECT_HEIGHT: u32 = 1;
const NATURAL_EARTH_BASEMAP_PNG: &[u8] = include_bytes!("../assets/maps/ne_basemap.png");

/// Cached world map background (generated once per session)
static WORLD_BACKGROUND_CACHE: OnceLock<Vec<u8>> = OnceLock::new();

#[derive(Clone, Copy)]
struct GeoRect {
    min_lon: f64,
    max_lon: f64,
    min_lat: f64,
    max_lat: f64,
}

#[derive(Clone, Copy)]
struct GeoPoint {
    lon: f64,
    lat: f64,
}

#[derive(Clone, Copy)]
struct NamedRect {
    name: &'static str,
    rect: GeoRect,
}

const ASCII_WORLD_REGIONS: &[NamedRect] = &[
    NamedRect {
        name: "North America",
        rect: GeoRect {
            min_lon: -170.0,
            max_lon: -50.0,
            min_lat: 15.0,
            max_lat: 72.0,
        },
    },
    NamedRect {
        name: "South America",
        rect: GeoRect {
            min_lon: -82.0,
            max_lon: -35.0,
            min_lat: -55.0,
            max_lat: 12.0,
        },
    },
    NamedRect {
        name: "Europe",
        rect: GeoRect {
            min_lon: -12.0,
            max_lon: 40.0,
            min_lat: 35.0,
            max_lat: 72.0,
        },
    },
    NamedRect {
        name: "Africa",
        rect: GeoRect {
            min_lon: -20.0,
            max_lon: 55.0,
            min_lat: -35.0,
            max_lat: 37.0,
        },
    },
    NamedRect {
        name: "Asia",
        rect: GeoRect {
            min_lon: 25.0,
            max_lon: 180.0,
            min_lat: 5.0,
            max_lat: 75.0,
        },
    },
    NamedRect {
        name: "Oceania",
        rect: GeoRect {
            min_lon: 110.0,
            max_lon: 180.0,
            min_lat: -50.0,
            max_lat: 20.0,
        },
    },
    NamedRect {
        name: "Antarctica",
        rect: GeoRect {
            min_lon: -180.0,
            max_lon: 180.0,
            min_lat: -90.0,
            max_lat: -62.0,
        },
    },
];

const NORTH_AMERICA_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: -168.0,
        lat: 72.0,
    },
    GeoPoint {
        lon: -150.0,
        lat: 70.0,
    },
    GeoPoint {
        lon: -132.0,
        lat: 60.0,
    },
    GeoPoint {
        lon: -125.0,
        lat: 50.0,
    },
    GeoPoint {
        lon: -117.0,
        lat: 33.0,
    },
    GeoPoint {
        lon: -108.0,
        lat: 26.0,
    },
    GeoPoint {
        lon: -96.0,
        lat: 18.0,
    },
    GeoPoint {
        lon: -83.0,
        lat: 9.0,
    },
    GeoPoint {
        lon: -79.0,
        lat: 18.0,
    },
    GeoPoint {
        lon: -83.0,
        lat: 25.0,
    },
    GeoPoint {
        lon: -96.0,
        lat: 30.0,
    },
    GeoPoint {
        lon: -88.0,
        lat: 46.0,
    },
    GeoPoint {
        lon: -74.0,
        lat: 48.0,
    },
    GeoPoint {
        lon: -58.0,
        lat: 53.0,
    },
    GeoPoint {
        lon: -60.0,
        lat: 62.0,
    },
    GeoPoint {
        lon: -82.0,
        lat: 73.0,
    },
    GeoPoint {
        lon: -120.0,
        lat: 73.0,
    },
];

const GREENLAND_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: -73.0,
        lat: 59.0,
    },
    GeoPoint {
        lon: -45.0,
        lat: 60.0,
    },
    GeoPoint {
        lon: -20.0,
        lat: 71.0,
    },
    GeoPoint {
        lon: -26.0,
        lat: 83.0,
    },
    GeoPoint {
        lon: -55.0,
        lat: 82.0,
    },
    GeoPoint {
        lon: -71.0,
        lat: 74.0,
    },
];

const SOUTH_AMERICA_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: -81.0,
        lat: 12.0,
    },
    GeoPoint {
        lon: -74.0,
        lat: 6.0,
    },
    GeoPoint {
        lon: -69.0,
        lat: -6.0,
    },
    GeoPoint {
        lon: -64.0,
        lat: -16.0,
    },
    GeoPoint {
        lon: -60.0,
        lat: -26.0,
    },
    GeoPoint {
        lon: -56.0,
        lat: -38.0,
    },
    GeoPoint {
        lon: -64.0,
        lat: -54.0,
    },
    GeoPoint {
        lon: -74.0,
        lat: -48.0,
    },
    GeoPoint {
        lon: -78.0,
        lat: -20.0,
    },
    GeoPoint {
        lon: -80.0,
        lat: -4.0,
    },
];

const EURASIA_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: -12.0,
        lat: 36.0,
    },
    GeoPoint {
        lon: -6.0,
        lat: 44.0,
    },
    GeoPoint {
        lon: 4.0,
        lat: 50.0,
    },
    GeoPoint {
        lon: 22.0,
        lat: 60.0,
    },
    GeoPoint {
        lon: 42.0,
        lat: 70.0,
    },
    GeoPoint {
        lon: 80.0,
        lat: 75.0,
    },
    GeoPoint {
        lon: 132.0,
        lat: 74.0,
    },
    GeoPoint {
        lon: 178.0,
        lat: 68.0,
    },
    GeoPoint {
        lon: 160.0,
        lat: 56.0,
    },
    GeoPoint {
        lon: 136.0,
        lat: 48.0,
    },
    GeoPoint {
        lon: 122.0,
        lat: 36.0,
    },
    GeoPoint {
        lon: 116.0,
        lat: 24.0,
    },
    GeoPoint {
        lon: 104.0,
        lat: 16.0,
    },
    GeoPoint {
        lon: 94.0,
        lat: 6.0,
    },
    GeoPoint {
        lon: 78.0,
        lat: 8.0,
    },
    GeoPoint {
        lon: 66.0,
        lat: 24.0,
    },
    GeoPoint {
        lon: 52.0,
        lat: 28.0,
    },
    GeoPoint {
        lon: 38.0,
        lat: 34.0,
    },
    GeoPoint {
        lon: 28.0,
        lat: 40.0,
    },
    GeoPoint {
        lon: 14.0,
        lat: 43.0,
    },
    GeoPoint {
        lon: 2.0,
        lat: 42.0,
    },
];

const AFRICA_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: -18.0,
        lat: 36.0,
    },
    GeoPoint {
        lon: -4.0,
        lat: 37.0,
    },
    GeoPoint {
        lon: 12.0,
        lat: 34.0,
    },
    GeoPoint {
        lon: 25.0,
        lat: 31.0,
    },
    GeoPoint {
        lon: 34.0,
        lat: 24.0,
    },
    GeoPoint {
        lon: 42.0,
        lat: 11.0,
    },
    GeoPoint {
        lon: 50.0,
        lat: -12.0,
    },
    GeoPoint {
        lon: 42.0,
        lat: -28.0,
    },
    GeoPoint {
        lon: 20.0,
        lat: -35.0,
    },
    GeoPoint {
        lon: 8.0,
        lat: -34.0,
    },
    GeoPoint {
        lon: -8.0,
        lat: -12.0,
    },
    GeoPoint {
        lon: -17.0,
        lat: 8.0,
    },
];

const AUSTRALIA_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: 112.0,
        lat: -10.0,
    },
    GeoPoint {
        lon: 128.0,
        lat: -12.0,
    },
    GeoPoint {
        lon: 142.0,
        lat: -16.0,
    },
    GeoPoint {
        lon: 154.0,
        lat: -28.0,
    },
    GeoPoint {
        lon: 150.0,
        lat: -38.0,
    },
    GeoPoint {
        lon: 134.0,
        lat: -43.0,
    },
    GeoPoint {
        lon: 116.0,
        lat: -34.0,
    },
];

const MADAGASCAR_POLYGON: &[GeoPoint] = &[
    GeoPoint {
        lon: 43.0,
        lat: -12.0,
    },
    GeoPoint {
        lon: 51.0,
        lat: -14.0,
    },
    GeoPoint {
        lon: 50.0,
        lat: -25.0,
    },
    GeoPoint {
        lon: 45.0,
        lat: -26.0,
    },
];

/// Generate a world map background image (cached after first generation)
pub fn generate_world_background() -> DynamicImage {
    let cached_bytes = WORLD_BACKGROUND_CACHE.get_or_init(|| {
        load_embedded_basemap()
            .unwrap_or_else(generate_fallback_background)
            .into_raw()
    });

    let img = RgbaImage::from_raw(TILE_SIZE, TILE_SIZE, cached_bytes.clone())
        .expect("cached image data is valid");
    DynamicImage::ImageRgba8(img)
}

fn load_embedded_basemap() -> Option<RgbaImage> {
    let image = image::load_from_memory(NATURAL_EARTH_BASEMAP_PNG).ok()?;
    Some(image::imageops::resize(
        &image.to_rgba8(),
        TILE_SIZE,
        TILE_SIZE,
        FilterType::CatmullRom,
    ))
}

fn generate_fallback_background() -> RgbaImage {
    let mut img = RgbaImage::new(TILE_SIZE, TILE_SIZE);
    render_ocean(&mut img);
    draw_graticule(&mut img);
    draw_continents_fast(&mut img);
    accent_coastlines(&mut img);
    img
}

fn render_ocean(img: &mut RgbaImage) {
    let (width, height) = img.dimensions();
    for y in 0..height {
        for x in 0..width {
            img.put_pixel(x, y, ocean_color_at_for_size(x, y, width, height));
        }
    }
}

fn ocean_color_at_for_size(x: u32, y: u32, width: u32, height: u32) -> Rgba<u8> {
    let x_norm = x as f32 / (width.saturating_sub(1).max(1)) as f32;
    let y_norm = y as f32 / (height.saturating_sub(1).max(1)) as f32;

    let equatorial_light = (1.0 - ((y_norm - 0.52).abs() * 1.85)).clamp(0.0, 1.0);
    let current_wave = (((x_norm * 13.0).sin() + (y_norm * 17.0).cos()) * 0.5
        + ((x_norm + y_norm) * 9.0).sin() * 0.35
        + 1.35)
        / 2.7;
    let blend = (0.18 + equatorial_light * 0.38 + current_wave * 0.28).clamp(0.0, 1.0);

    let contour = ((x_norm * 26.0 + y_norm * 18.0 + (y_norm * 7.0).sin() * 1.8).sin()).abs();
    let contour_boost = if contour > 0.988 {
        12
    } else if contour > 0.97 {
        6
    } else {
        0
    };

    let base = if blend < 0.55 {
        lerp_rgba(OCEAN_DEEP_COLOR, OCEAN_COLOR, blend / 0.55, 0)
    } else {
        lerp_rgba(OCEAN_COLOR, OCEAN_SHALLOW_COLOR, (blend - 0.55) / 0.45, 0)
    };

    brighten_rgba(base, contour_boost)
}

fn lerp_rgba(from: Rgba<u8>, to: Rgba<u8>, t: f32, brighten: u8) -> Rgba<u8> {
    let lerp = |start: u8, end: u8| -> u8 {
        let mixed = start as f32 + (end as f32 - start as f32) * t;
        mixed.round().clamp(0.0, 255.0) as u8
    };

    Rgba([
        lerp(from.0[0], to.0[0]).saturating_add(brighten),
        lerp(from.0[1], to.0[1]).saturating_add(brighten),
        lerp(from.0[2], to.0[2]).saturating_add(brighten / 2),
        255,
    ])
}

fn brighten_rgba(color: Rgba<u8>, brighten: u8) -> Rgba<u8> {
    Rgba([
        color.0[0].saturating_add(brighten),
        color.0[1].saturating_add(brighten),
        color.0[2].saturating_add(brighten / 2),
        color.0[3],
    ])
}

/// Convert longitude (-180 to 180) to pixel x coordinate
fn lon_to_x_for_width(lon: f64, width: u32) -> u32 {
    ((lon + 180.0) / 360.0 * width as f64).clamp(0.0, width.saturating_sub(1) as f64) as u32
}

/// Convert latitude (-85 to 85, Web Mercator) to pixel y coordinate
fn lat_to_y_for_height(lat: f64, height: u32) -> u32 {
    // Web Mercator projection (simplified for our zoom level 0)
    let lat_rad = lat.to_radians();
    let merc_y = lat_rad.tan().asinh();
    // Normalize to 0-1 range (approx for zoom 0)
    let normalized = 0.5 - merc_y / (2.0 * std::f64::consts::PI);
    (normalized * height as f64).clamp(0.0, height.saturating_sub(1) as f64) as u32
}

fn pixel_x_to_lon(x: u32, width: u32) -> f64 {
    (x as f64 / width.saturating_sub(1).max(1) as f64) * 360.0 - 180.0
}

fn pixel_y_to_lat(y: u32, height: u32) -> f64 {
    let normalized = y as f64 / height.saturating_sub(1).max(1) as f64;
    let merc_y = (0.5 - normalized) * (2.0 * std::f64::consts::PI);
    merc_y.sinh().atan().to_degrees()
}

#[cfg(test)]
fn lon_to_x(lon: f64) -> u32 {
    lon_to_x_for_width(lon, TILE_SIZE)
}

#[cfg(test)]
fn lat_to_y(lat: f64) -> u32 {
    lat_to_y_for_height(lat, TILE_SIZE)
}

/// Draw a filled rectangle using geographic coordinates
fn draw_rect(
    img: &mut RgbaImage,
    min_lon: f64,
    max_lon: f64,
    min_lat: f64,
    max_lat: f64,
    color: Rgba<u8>,
) {
    let (width, height) = img.dimensions();
    let x1 = lon_to_x_for_width(min_lon, width);
    let x2 = lon_to_x_for_width(max_lon, width);
    let y1 = lat_to_y_for_height(max_lat, height); // Note: y is inverted (max_lat = lower y)
    let y2 = lat_to_y_for_height(min_lat, height);

    for y in y1.min(y2)..=y1.max(y2) {
        for x in x1.min(x2)..=x1.max(x2) {
            if x < width && y < height {
                img.put_pixel(x, y, color);
            }
        }
    }
}

fn draw_polygon(img: &mut RgbaImage, polygon: &[GeoPoint], color: Rgba<u8>) {
    if polygon.len() < 3 {
        return;
    }

    let (width, height) = img.dimensions();
    let min_lon = polygon
        .iter()
        .map(|point| point.lon)
        .fold(f64::INFINITY, f64::min);
    let max_lon = polygon
        .iter()
        .map(|point| point.lon)
        .fold(f64::NEG_INFINITY, f64::max);
    let min_lat = polygon
        .iter()
        .map(|point| point.lat)
        .fold(f64::INFINITY, f64::min);
    let max_lat = polygon
        .iter()
        .map(|point| point.lat)
        .fold(f64::NEG_INFINITY, f64::max);

    let min_x = lon_to_x_for_width(min_lon, width);
    let max_x = lon_to_x_for_width(max_lon, width);
    let min_y = lat_to_y_for_height(max_lat, height);
    let max_y = lat_to_y_for_height(min_lat, height);

    for y in min_y.min(max_y)..=min_y.max(max_y) {
        for x in min_x.min(max_x)..=min_x.max(max_x) {
            let lon = pixel_x_to_lon(x, width);
            let lat = pixel_y_to_lat(y, height);
            if point_in_polygon(lon, lat, polygon) {
                img.put_pixel(x, y, color);
            }
        }
    }
}

fn point_in_polygon(lon: f64, lat: f64, polygon: &[GeoPoint]) -> bool {
    let mut inside = false;
    let mut j = polygon.len() - 1;

    for i in 0..polygon.len() {
        let point_i = polygon[i];
        let point_j = polygon[j];
        let lat_cross = (point_i.lat > lat) != (point_j.lat > lat);
        if lat_cross {
            let lon_intersection = (point_j.lon - point_i.lon) * (lat - point_i.lat)
                / ((point_j.lat - point_i.lat).abs().max(f64::EPSILON))
                + point_i.lon;
            if lon < lon_intersection {
                inside = !inside;
            }
        }
        j = i;
    }

    inside
}

fn draw_graticule(img: &mut RgbaImage) {
    let (width, height) = img.dimensions();
    for lon in (-150..=150).step_by(30) {
        let x = lon_to_x_for_width(lon as f64, width);
        for y in 0..height {
            img.put_pixel(x, y, OCEAN_GRID_COLOR);
        }
    }

    for lat in (-60..=60).step_by(30) {
        let y = lat_to_y_for_height(lat as f64, height);
        for x in 0..width {
            img.put_pixel(x, y, OCEAN_GRID_COLOR);
        }
    }
}

/// Draw continents using coarse polygon landmasses rather than boxy rectangles.
fn draw_continents_fast(img: &mut RgbaImage) {
    draw_polygon(img, NORTH_AMERICA_POLYGON, LAND_COLOR);
    draw_polygon(img, GREENLAND_POLYGON, LAND_COLOR);
    draw_polygon(img, SOUTH_AMERICA_POLYGON, LAND_COLOR);
    draw_polygon(img, EURASIA_POLYGON, LAND_COLOR);
    draw_polygon(img, AFRICA_POLYGON, LAND_COLOR);
    draw_polygon(img, AUSTRALIA_POLYGON, LAND_COLOR);
    draw_polygon(img, MADAGASCAR_POLYGON, LAND_COLOR);

    // Major archipelagos and islands that help the global silhouette read correctly.
    draw_rect(img, 95.0, 141.0, -10.0, 20.0, LAND_COLOR); // Maritime Southeast Asia
    draw_rect(img, 128.0, 146.0, 30.0, 46.0, LAND_COLOR); // Japan
    draw_rect(img, 166.0, 179.0, -47.0, -34.0, LAND_COLOR); // New Zealand
    draw_rect(img, -11.0, 2.0, 50.0, 59.0, LAND_COLOR); // UK + Ireland
    draw_rect(img, -25.0, -13.0, 63.0, 67.0, LAND_COLOR); // Iceland
    draw_rect(img, 79.0, 82.0, 6.0, 10.0, LAND_COLOR); // Sri Lanka
    draw_rect(img, 120.0, 123.0, 22.0, 25.0, LAND_COLOR); // Taiwan
    draw_rect(img, 141.0, 155.0, -11.0, -1.0, LAND_COLOR); // Papua New Guinea
    draw_rect(img, -85.0, -74.0, 18.0, 24.0, LAND_COLOR); // Caribbean / Cuba
    draw_rect(img, -180.0, 180.0, -90.0, -62.0, LAND_COLOR); // Antarctica
}

fn accent_coastlines(img: &mut RgbaImage) {
    let snapshot = img.clone();
    let (width, height) = snapshot.dimensions();

    for y in 1..height.saturating_sub(1) {
        for x in 1..width.saturating_sub(1) {
            if *snapshot.get_pixel(x, y) != LAND_COLOR {
                continue;
            }

            let touches_ocean = [
                snapshot.get_pixel(x - 1, y),
                snapshot.get_pixel(x + 1, y),
                snapshot.get_pixel(x, y - 1),
                snapshot.get_pixel(x, y + 1),
            ]
            .into_iter()
            .any(|neighbor| *neighbor != LAND_COLOR);

            if touches_ocean {
                img.put_pixel(x, y, COASTLINE_COLOR);
            }
        }
    }
}

/// Composite the GBIF occurrence layer on top of the world background
pub fn composite_with_background(occurrence_layer: &DynamicImage) -> DynamicImage {
    let overlay = occurrence_layer.to_rgba8();
    let mut result = if overlay.width() == TILE_SIZE && overlay.height() == TILE_SIZE {
        generate_world_background().to_rgba8()
    } else {
        image::imageops::resize(
            &generate_world_background().to_rgba8(),
            overlay.width(),
            overlay.height(),
            FilterType::Triangle,
        )
    };

    // Composite overlay on top of background using alpha blending
    for (x, y, pixel) in overlay.enumerate_pixels() {
        if pixel.0[3] > 0 {
            // Only composite non-transparent pixels
            let bg_pixel = result.get_pixel(x, y);
            let alpha = pixel.0[3] as f32 / 255.0;
            let inv_alpha = 1.0 - alpha;

            let r = (pixel.0[0] as f32 * alpha + bg_pixel.0[0] as f32 * inv_alpha) as u8;
            let g = (pixel.0[1] as f32 * alpha + bg_pixel.0[1] as f32 * inv_alpha) as u8;
            let b = (pixel.0[2] as f32 * alpha + bg_pixel.0[2] as f32 * inv_alpha) as u8;

            result.put_pixel(x, y, Rgba([r, g, b, 255]));
        }
    }

    DynamicImage::ImageRgba8(result)
}

/// Normalize range maps for terminal presentation so they read as a wide world map
/// rather than a square tile.
pub fn normalize_for_tui(map_image: &DynamicImage) -> DynamicImage {
    let width = map_image.width();
    let height = map_image.height();

    if width == 0 || height == 0 {
        return map_image.clone();
    }

    let target_width = height.saturating_mul(TUI_MAP_ASPECT_WIDTH) / TUI_MAP_ASPECT_HEIGHT;

    if width == target_width {
        return map_image.clone();
    }

    DynamicImage::ImageRgba8(image::imageops::resize(
        &map_image.to_rgba8(),
        target_width.max(1),
        height.max(1),
        FilterType::CatmullRom,
    ))
}

/// Stretch a range map to the exact terminal render slot so the TUI panel
/// does not leave unused side padding.
pub fn stretch_for_terminal_area(
    map_image: &DynamicImage,
    area_width: u16,
    area_height: u16,
    font_size: (u16, u16),
) -> DynamicImage {
    let pixel_width = u32::from(area_width).saturating_mul(u32::from(font_size.0.max(1)));
    let pixel_height = u32::from(area_height).saturating_mul(u32::from(font_size.1.max(1)));

    if pixel_width == 0 || pixel_height == 0 {
        return map_image.clone();
    }

    if map_image.width() == pixel_width && map_image.height() == pixel_height {
        return map_image.clone();
    }

    DynamicImage::ImageRgba8(image::imageops::resize(
        &map_image.to_rgba8(),
        pixel_width,
        pixel_height,
        FilterType::CatmullRom,
    ))
}

/// Generate a compact ASCII world map for TUI fallback rendering.
pub fn generate_ascii_range_map(
    width: usize,
    height: usize,
    bounding_box: Option<(f64, f64, f64, f64)>,
    continents: &[String],
) -> Vec<String> {
    let width = width.max(12);
    let height = height.max(4);
    let mut grid = vec![vec!['~'; width]; height];

    for region in ASCII_WORLD_REGIONS {
        draw_ascii_rect(&mut grid, region.rect, '.');
    }

    if let Some((min_lat, max_lat, min_lon, max_lon)) = bounding_box {
        draw_ascii_rect(
            &mut grid,
            GeoRect {
                min_lon,
                max_lon,
                min_lat,
                max_lat,
            },
            '#',
        );
    } else {
        for continent in continents {
            for region in ASCII_WORLD_REGIONS
                .iter()
                .filter(|region| continent_matches(continent, region.name))
            {
                draw_ascii_rect(&mut grid, region.rect, '#');
            }
        }
    }

    grid.into_iter()
        .map(|row| row.into_iter().collect::<String>())
        .collect()
}

fn draw_ascii_rect(grid: &mut [Vec<char>], rect: GeoRect, fill: char) {
    if grid.is_empty() || grid[0].is_empty() {
        return;
    }

    let width = grid[0].len();
    let height = grid.len();

    let x1 = lon_to_col(rect.min_lon, width);
    let x2 = lon_to_col(rect.max_lon, width);
    let y1 = lat_to_row(rect.max_lat, height);
    let y2 = lat_to_row(rect.min_lat, height);

    let row_start = y1.min(y2);
    let row_end = y1.max(y2).min(height.saturating_sub(1));
    let col_start = x1.min(x2);
    let col_end = x1.max(x2).min(width.saturating_sub(1));

    for row in grid.iter_mut().take(row_end + 1).skip(row_start) {
        for cell in row.iter_mut().take(col_end + 1).skip(col_start) {
            *cell = fill;
        }
    }
}

fn lon_to_col(lon: f64, width: usize) -> usize {
    ((lon + 180.0) / 360.0 * (width.saturating_sub(1)) as f64)
        .clamp(0.0, width.saturating_sub(1) as f64) as usize
}

fn lat_to_row(lat: f64, height: usize) -> usize {
    ((90.0 - lat) / 180.0 * (height.saturating_sub(1)) as f64)
        .clamp(0.0, height.saturating_sub(1) as f64) as usize
}

fn continent_matches(input: &str, name: &str) -> bool {
    let normalized = input.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "north america" => name == "North America",
        "south america" => name == "South America",
        "europe" => name == "Europe",
        "africa" => name == "Africa",
        "asia" => name == "Asia",
        "oceania" | "australia" => name == "Oceania",
        "antarctica" => name == "Antarctica",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::GenericImageView;

    #[test]
    fn test_world_background_generation() {
        let img = generate_world_background();
        assert_eq!(img.width(), 512);
        assert_eq!(img.height(), 512);

        let pixel = img.get_pixel(lon_to_x(-145.0), lat_to_y(12.0));
        assert_eq!(pixel.0[3], 255, "Basemap pixels should be opaque");
        assert_ne!(
            pixel.0, LAND_COLOR.0,
            "Known ocean point should not render as land"
        );
    }

    #[test]
    fn test_world_background_has_visible_land_contrast() {
        let img = generate_world_background();

        // Central Africa should render as land, not ocean.
        let pixel = img.get_pixel(lon_to_x(20.0), lat_to_y(0.0));
        assert_ne!(pixel.0, OCEAN_COLOR.0, "Land should contrast with ocean");
    }

    #[test]
    fn test_world_background_ocean_gradient_varies_by_region() {
        let img = generate_world_background();
        let pacific = img.get_pixel(lon_to_x(-145.0), lat_to_y(12.0));
        let south_atlantic = img.get_pixel(lon_to_x(-10.0), lat_to_y(-38.0));
        assert_ne!(
            pacific.0, south_atlantic.0,
            "Ocean should not be a flat single-color fill"
        );
    }

    #[test]
    fn test_composite_accepts_non_default_overlay_size() {
        let overlay =
            DynamicImage::ImageRgba8(RgbaImage::from_pixel(640, 320, Rgba([255, 221, 0, 180])));

        let composited = composite_with_background(&overlay);

        assert_eq!(composited.width(), 640);
        assert_eq!(composited.height(), 320);
    }

    #[test]
    fn test_coordinate_conversion() {
        // Test longitude conversion
        assert_eq!(lon_to_x(-180.0), 0);
        assert_eq!(lon_to_x(0.0), 256);
        assert_eq!(lon_to_x(180.0), 511);

        // Test latitude conversion (Web Mercator)
        assert!(lat_to_y(0.0) > 200 && lat_to_y(0.0) < 300); // Near middle
        assert!(lat_to_y(60.0) < lat_to_y(0.0)); // North should be lower y
        assert!(lat_to_y(-60.0) > lat_to_y(0.0)); // South should be higher y
    }

    #[test]
    fn test_generation_is_fast() {
        use std::time::Instant;
        let start = Instant::now();
        let _img = generate_world_background();
        let elapsed = start.elapsed();
        // Should complete in under 1 second
        assert!(
            elapsed.as_secs() < 1,
            "Generation took {:?}, should be under 1 second",
            elapsed
        );
    }

    #[test]
    fn test_ascii_range_map_highlights_bounding_box() {
        let map = generate_ascii_range_map(24, 8, Some((-35.0, 35.0, -20.0, 55.0)), &[]);
        assert!(map.iter().any(|line| line.contains('#')));
    }

    #[test]
    fn test_ascii_range_map_highlights_continent_when_bbox_missing() {
        let map = generate_ascii_range_map(24, 8, None, &[String::from("Africa")]);
        assert!(map.iter().any(|line| line.contains('#')));
    }
}
