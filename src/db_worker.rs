use crate::api::wikipedia::{WikiArticle, WikiLifeHistoryFallback};
use crate::local_db::{CachedMedia, CachedSpecies, LocalDatabase, TaxonName};
use crate::species::UnifiedSpecies;
use std::io;
use std::sync::mpsc;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce(&mut LocalDatabase) + Send + 'static>;

#[derive(Clone)]
pub struct DbWorker {
    sender: mpsc::Sender<Job>,
}

impl DbWorker {
    pub fn new() -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();

        std::thread::Builder::new()
            .name("ncbi-poketext-db".to_string())
            .spawn(move || {
                let mut db = match LocalDatabase::open() {
                    Ok(db) => {
                        let _ = ready_tx.send(Ok(()));
                        db
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };

                while let Ok(job) = receiver.recv() {
                    job(&mut db);
                }
            })
            .map_err(io::Error::other)?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { sender }),
            Ok(Err(message)) => Err(io::Error::other(message)),
            Err(error) => Err(io::Error::other(error.to_string())),
        }
    }

    async fn request<R, F>(&self, operation: F) -> Option<R>
    where
        R: Send + 'static,
        F: FnOnce(&mut LocalDatabase) -> R + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Box::new(move |db| {
                let result = operation(db);
                let _ = reply_tx.send(result);
            }))
            .ok()?;

        reply_rx.await.ok()
    }

    fn enqueue<F>(&self, operation: F)
    where
        F: FnOnce(&mut LocalDatabase) + Send + 'static,
    {
        let _ = self.sender.send(Box::new(operation));
    }

    pub async fn get_species(&self, scientific_name: String) -> Option<CachedSpecies> {
        self.request(move |db| db.get_species(&scientific_name).ok().flatten())
            .await
            .flatten()
    }

    pub async fn get_cached_media(
        &self,
        species_image_url: Option<String>,
        gbif_key: Option<u64>,
    ) -> CachedMedia {
        self.request(move |db| {
            db.get_cached_media(species_image_url.as_deref(), gbif_key)
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    pub async fn get_rich_species(&self, scientific_name: String) -> Option<UnifiedSpecies> {
        self.request(move |db| db.get_rich_species(&scientific_name).ok().flatten())
            .await
            .flatten()
    }

    pub fn cache_rich_species_detached(&self, species: UnifiedSpecies) {
        self.enqueue(move |db| {
            let _ = db.cache_rich_species(&species);
        });
    }

    pub async fn get_wiki_article(&self, title: String) -> Option<WikiArticle> {
        self.request(move |db| db.get_wiki_article(&title).ok().flatten())
            .await
            .flatten()
    }

    pub fn cache_wiki_article_detached(&self, title: String, article: WikiArticle) {
        self.enqueue(move |db| {
            let _ = db.cache_wiki_article(&title, &article);
        });
    }

    pub async fn get_wiki_life_history(&self, title: String) -> Option<WikiLifeHistoryFallback> {
        self.request(move |db| db.get_wiki_life_history(&title).ok().flatten())
            .await
            .flatten()
    }

    pub fn cache_wiki_life_history_detached(
        &self,
        title: String,
        fallback: WikiLifeHistoryFallback,
    ) {
        self.enqueue(move |db| {
            let _ = db.cache_wiki_life_history(&title, &fallback);
        });
    }

    pub fn cache_species_detached(&self, species: UnifiedSpecies) {
        self.enqueue(move |db| {
            let _ = db.cache_species(&species);
        });
    }

    pub async fn invalidate_species(&self, scientific_name: String) {
        let _ = self
            .request(move |db| db.invalidate_species(&scientific_name).ok())
            .await;
    }

    pub async fn cache_species_image(&self, species: UnifiedSpecies, data: Vec<u8>) {
        let _ = self
            .request(move |db| db.cache_species_image(&species, &data).ok())
            .await;
    }

    pub fn cache_image_detached(
        &self,
        url: String,
        data: Vec<u8>,
        content_type: Option<&'static str>,
    ) {
        self.enqueue(move |db| {
            let _ = db.cache_image(&url, &data, content_type);
        });
    }

    pub async fn cache_map_image(&self, gbif_key: u64, data: Vec<u8>) {
        let _ = self
            .request(move |db| db.cache_map_image(gbif_key, &data).ok())
            .await;
    }

    pub fn cache_map_image_detached(&self, gbif_key: u64, data: Vec<u8>) {
        self.enqueue(move |db| {
            let _ = db.cache_map_image(gbif_key, &data);
        });
    }

    pub async fn flush(&self) {
        let _ = self.request(|_| ()).await;
    }

    pub async fn invalidate_map_image(&self, gbif_key: u64) {
        let _ = self
            .request(move |db| db.invalidate_map_image(gbif_key).ok())
            .await;
    }

    pub async fn search_taxon_names(&self, query: String, limit: u32) -> Vec<TaxonName> {
        self.request(move |db| db.search_taxon_names(&query, limit).unwrap_or_default())
            .await
            .unwrap_or_default()
    }

    pub async fn get_species_batch_after(&self, after_gbif_key: u64, limit: u32) -> Vec<TaxonName> {
        self.request(move |db| {
            db.get_species_batch_after(after_gbif_key, limit)
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    pub async fn get_cached_kingdoms(&self) -> Vec<String> {
        self.request(|db| db.get_cached_kingdoms().unwrap_or_default())
            .await
            .unwrap_or_default()
    }

    pub async fn get_cached_parent_taxon(
        &self,
        child_rank: String,
        child_value: String,
    ) -> Option<(String, String)> {
        self.request(move |db| {
            db.get_cached_parent_taxon(&child_rank, &child_value)
                .ok()
                .flatten()
        })
        .await
        .flatten()
    }

    pub async fn has_backbone(&self) -> bool {
        self.request(|db| db.has_backbone()).await.unwrap_or(false)
    }

    pub async fn species_rank_count(&self) -> u64 {
        self.request(|db| db.species_rank_count().unwrap_or(0))
            .await
            .unwrap_or(0)
    }

    pub async fn get_user_stat(&self, key: String) -> Option<String> {
        self.request(move |db| db.get_user_stat(&key).ok().flatten())
            .await
            .flatten()
    }

    pub async fn set_user_stat(&self, key: String, value: String) {
        let _ = self
            .request(move |db| db.set_user_stat(&key, &value).ok())
            .await;
    }

    pub async fn delete_user_stat(&self, key: String) {
        let _ = self.request(move |db| db.delete_user_stat(&key).ok()).await;
    }

    pub async fn toggle_favorite(&self, scientific_name: String) -> bool {
        self.request(move |db| {
            if db.is_favorite(&scientific_name).unwrap_or(false) {
                let _ = db.remove_favorite(&scientific_name);
                false
            } else {
                let _ = db.add_favorite(&scientific_name, None);
                true
            }
        })
        .await
        .unwrap_or(false)
    }

    pub async fn is_favorite(&self, scientific_name: String) -> bool {
        self.request(move |db| db.is_favorite(&scientific_name).unwrap_or(false))
            .await
            .unwrap_or(false)
    }

    pub async fn get_siblings(
        &self,
        parent_rank: String,
        parent_value: String,
        child_rank: String,
        limit: u32,
    ) -> Vec<TaxonName> {
        self.request(move |db| {
            db.get_siblings(&parent_rank, &parent_value, &child_rank, limit)
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    pub async fn get_species_in_genus(&self, genus: String, limit: u32) -> Vec<TaxonName> {
        self.request(move |db| db.get_species_in_genus(&genus, limit).unwrap_or_default())
            .await
            .unwrap_or_default()
    }

    pub async fn get_genera_in_family(&self, family: String, limit: u32) -> Vec<TaxonName> {
        self.request(move |db| db.get_genera_in_family(&family, limit).unwrap_or_default())
            .await
            .unwrap_or_default()
    }

    pub async fn get_taxon_by_name(&self, name: String) -> Option<TaxonName> {
        self.request(move |db| db.get_taxon_by_name(&name).ok().flatten())
            .await
            .flatten()
    }
}
