//! Optional, read-only community segment lookup. Playback never waits for this worker. It owns
//! a bounded latest-request queue and cache; only a current, addressed landing may enrich the
//! playing leaf. No Plex credential, title, media bytes or submission is sent to either service.
// Fixture builds deliberately suppress the worker's network root. Its production helpers remain
// compiled so the same parser/queue code is exercised by deterministic tests, without public I/O.
#![cfg_attr(any(test, feature = "test-support"), allow(dead_code))]
use super::{Marker, MarkerKind, PlayingItem};
use plx_plex::plex::{self, ServerId};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const KINDS: [MarkerKind; 3] = [MarkerKind::Intro, MarkerKind::Recap, MarkerKind::Credits];
const MAX_BODY: usize = 64 * 1024;
const MAX_TIME_MS: i64 = 6 * 60 * 60 * 1000;
const CACHE_CAP: usize = 64;
const RESULT_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const MISS_TTL: Duration = Duration::from_secs(10 * 60);

/// None = no usable answer; Some(empty) = explicit no-segment. The latter must stop fallback.
/// The flag means a known after-credits scene makes the synthetic tail unsafe.
#[derive(Clone, Default)]
struct Segments([Option<Vec<Marker>>; 3], bool);

impl Segments {
    fn fill_missing(&mut self, fallback: Self) {
        self.1 |= fallback.1;
        for (target, source) in self.0.iter_mut().zip(fallback.0) {
            if target.is_none() { *target = source; }
        }
    }
    fn merge_into(&self, markers: &mut Vec<Marker>) -> bool {
        let before = markers.len();
        for (kind, segments) in KINDS.into_iter().zip(&self.0) {
            // A Plex marker owns its entire kind, including multiple credits around a scene.
            if markers.iter().any(|m| m.kind == kind) { continue; }
            if let Some(segments) = segments { markers.extend_from_slice(segments); }
        }
        markers.len() != before
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Capture {
    sid: ServerId,
    rk: String,
    machine: String,
    client: u32,
    token: u32,
    profile: u32,
    user: String,
    duration_ms: i64,
}

impl Capture {
    fn current(&self) -> bool {
        let profile = plex::session::current_snapshot();
        profile.generation == self.profile
            && plex::session::current_profile_key() == self.user
            && plex::client_for(self.sid).is_some_and(|c| {
                c.instance_gen() == self.client && c.token_gen() == self.token
                    && c.machine_id() == self.machine
            })
    }
}

#[derive(Clone)]
struct Request { generation: u64, capture: Capture }

pub(super) struct Answer { request: Request, segments: Segments }

impl Answer {
    pub(super) fn matches(&self, playing: &PlayingItem) -> bool {
        self.request.capture.sid == playing.sid && self.request.capture.rk == playing.rk
    }
    pub(super) fn no_credits(&self) -> bool {
        self.segments.1 || self.segments.0[2].as_ref().is_some_and(Vec::is_empty)
    }
    pub(super) fn merge_into(&self, markers: &mut Vec<Marker>) -> bool {
        self.segments.merge_into(markers)
    }
}

struct Cached { capture: Capture, expires: Instant, segments: Segments }

#[derive(Default)]
struct State {
    generation: u64,
    pending: Option<Request>,
    running: bool,
    answer: Option<Answer>,
    cache: VecDeque<Cached>,
}

impl State {
    /// The generation check and publication share one lock: an old episode completing after
    /// the next one started cannot overwrite either its landing or its cache record.
    fn publish(&mut self, request: Request, segments: Segments, now: Instant, remember: bool) -> bool {
        if self.generation != request.generation { return false; }
        if remember {
            let ttl = if segments.0.iter().any(Option::is_some) { RESULT_TTL } else { MISS_TTL };
            self.cache.retain(|entry| entry.capture != request.capture && entry.expires > now);
            self.cache.push_back(Cached { capture: request.capture.clone(), expires: now + ttl, segments: segments.clone() });
            while self.cache.len() > CACHE_CAP { self.cache.pop_front(); }
        }
        self.answer = Some(Answer { request, segments });
        true
    }
}

#[derive(Clone, Default)]
pub(super) struct Worker(Arc<Mutex<State>>);

impl Worker {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(super) fn cancel(&self) {
        let mut state = self.state();
        state.generation = state.generation.wrapping_add(1);
        state.pending = None;
        state.answer = None;
    }
    pub(super) fn start(&self, playing: Option<&PlayingItem>, duration_ms: i64) {
        self.cancel();
        let Some(playing) = playing else { return };
        if playing.rk.is_empty() || KINDS.iter().all(|kind| playing.markers.iter().any(|m| m.kind == *kind))
            || crate::stores::tape::active() { return; }
        let Some(client) = plex::client_for(playing.sid) else { return };
        if client.machine_id().is_empty() { return; }
        let capture = Capture {
            sid: playing.sid, rk: playing.rk.clone(), machine: client.machine_id().to_owned(),
            client: client.instance_gen(), token: client.token_gen(),
            profile: plex::session::current_snapshot().generation,
            user: plex::session::current_profile_key(), duration_ms,
        };
        // Host fixtures and replay-support dependents never contact public services. Tests below
        // drive parsing, precedence, cache and landing invalidation directly and deterministically.
        #[cfg(any(test, feature = "test-support"))]
        { let _ = capture; }
        #[cfg(not(any(test, feature = "test-support")))]
        {
            let spawn = {
                let mut state = self.state();
                state.pending = Some(Request { generation: state.generation, capture });
                !std::mem::replace(&mut state.running, true)
            };
            if spawn {
                let worker = self.clone();
                if !plx_base::task::spawn_small("skip-segments", move || worker.run()) {
                    let mut state = self.state();
                    state.running = false;
                    state.pending = None;
                }
            }
        }
    }
    fn is_current(&self, request: &Request) -> bool {
        // Release our mailbox lock before consulting session/server authorities. Neither lock
        // ordering nor a network request should ever depend on the owner holding this mutex.
        let generation = self.state().generation;
        generation == request.generation && request.capture.current()
    }
    fn cached(&self, capture: &Capture, now: Instant) -> Option<Segments> {
        let mut state = self.state();
        state.cache.retain(|entry| entry.expires > now);
        state.cache.iter().find(|entry| entry.capture == *capture).map(|e| e.segments.clone())
    }
    fn land(&self, request: Request, segments: Segments, now: Instant, remember: bool) {
        if !request.capture.current() { return; }
        let mut state = self.state();
        if !state.publish(request, segments, now, remember) { return; }
        drop(state);
        plx_machine::idle::invalidate();
    }
    fn run(&self) {
        loop {
            let request = {
                let mut state = self.state();
                let Some(request) = state.pending.take() else { state.running = false; return };
                request
            };
            if !self.is_current(&request) { continue; }
            let cached = self.cached(&request.capture, Instant::now());
            let remember = cached.is_none();
            let segments = cached.unwrap_or_else(|| {
                std::panic::catch_unwind(|| self.lookup(&request)).unwrap_or_default()
            });
            self.land(request, segments, Instant::now(), remember);
        }
    }
    pub(super) fn take(&self) -> Option<Answer> {
        let answer = self.state().answer.take()?;
        self.is_current(&answer.request).then_some(answer)
    }
    fn lookup(&self, request: &Request) -> Segments {
        let capture = &request.capture;
        let Some(client) = plex::client_for(capture.sid) else { return Segments::default() };
        let deadline = Instant::now() + Duration::from_millis(2500);
        let Some(leaf) = client.segment_metadata(&capture.rk, deadline) else { return Segments::default() };
        if !self.is_current(request) { return Segments::default(); }
        let duration_ms = leaf.duration;
        if !(1..=MAX_TIME_MS).contains(&duration_ms) { return Segments::default(); }
        let episode = match leaf.kind.as_str() {
            "episode" if leaf.parent_index > 0 && leaf.index > 0 && !leaf.grandparent_rating_key.is_empty() => Some((leaf.parent_index, leaf.index)),
            "movie" => None,
            _ => return Segments::default(),
        };
        // For an episode, only the SHOW's external identities may address a provider. An episode
        // IMDb ID is a different entity and using it with season/episode can return another show.
        let identity = if episode.is_some() {
            let Some(show) = client.segment_metadata(&leaf.grandparent_rating_key, deadline) else { return Segments::default() };
            if show.kind != "show" { return Segments::default(); }
            // The community APIs address canonical episodes, not Plex's DVD/absolute display
            // coordinates. Their public API has no episode-GUID resolver; TMDB /find requires
            // credentials. Only a positively established TMDB aired source can use raw S/E.
            let ordering = client.segment_show_ordering(&show, deadline);
            let Some(ids) = canonical_episode_ids(&show, ordering.as_deref()) else {
                let reason = if ordering.is_none() { "ordering unavailable" }
                    else if ordering.as_deref() != Some("tmdbAiring") { "ordering unsupported" }
                    else { "canonical identity unavailable" };
                plx_base::eventlog::log(&format!("community skip: {reason}"));
                return Segments::default();
            };
            ids
        } else { Ids::from_metadata(&leaf) };
        if !self.is_current(request) { return Segments::default(); }
        let query = Query { ids: identity, episode, duration_ms };
        let mut result = query.primary_url().and_then(|url| public_json(&url))
            .and_then(|body| parse_primary(&body, &query)).unwrap_or_default();
        if result.0.iter().any(Option::is_none) && self.is_current(request) {
            if let Some(fallback) = query.fallback_url().and_then(|url| public_json(&url))
                .and_then(|body| parse_fallback(&body, &query)) {
                result.fill_missing(fallback);
            }
        }
        let count: usize = result.0.iter().filter_map(Option::as_ref).map(Vec::len).sum();
        plx_base::eventlog::log(&format!("community skip: result segments={count}"));
        result
    }
}

fn canonical_episode_ids(show: &plex::Metadata, ordering: Option<&str>) -> Option<Ids> {
    if show.kind != "show" || ordering != Some("tmdbAiring") { return None; }
    let ids = Ids::from_metadata(show);
    ids.tmdb?; // Do not ask the primary API to guess a mapping from TVDB/IMDb coordinates.
    Some(ids)
}

#[derive(Default)]
struct Ids { tmdb: Option<u32>, tvdb: Option<u32>, imdb: Option<String> }

impl Ids {
    fn from_metadata(metadata: &plex::Metadata) -> Self {
        let mut ids = Self::default();
        // Old agents sometimes store an external ID in scalar guid. Accept only the exact
        // recognized scheme and validated ID, never a Plex guid or a title guess.
        for guid in metadata.external_guids.iter().map(|g| g.id.as_str()).chain(std::iter::once(metadata.guid.as_str())) {
            let Some((scheme, id)) = guid.split_once("://") else { continue };
            match scheme {
                "tmdb" => ids.tmdb = ids.tmdb.or_else(|| numeric_id(id)),
                "tvdb" => ids.tvdb = ids.tvdb.or_else(|| numeric_id(id)),
                "imdb" if valid_imdb(id) => { if ids.imdb.is_none() { ids.imdb = Some(id.to_owned()); } }
                _ => {}
            }
        }
        ids
    }
}
fn numeric_id(id: &str) -> Option<u32> {
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) { return None; }
    id.parse().ok().filter(|id| (1..=10_000_000).contains(id))
}
fn valid_imdb(id: &str) -> bool {
    id.strip_prefix("tt").is_some_and(|n| (7..=8).contains(&n.len()) && n.bytes().all(|b| b.is_ascii_digit()))
}

struct Query { ids: Ids, episode: Option<(i64, i64)>, duration_ms: i64 }

impl Query {
    fn address(&self) -> String {
        self.episode.map_or(String::new(), |(season, episode)| format!("&season={season}&episode={episode}"))
    }
    fn primary_url(&self) -> Option<String> {
        let id = if let Some(id) = self.ids.tmdb { format!("tmdb_id={id}") }
            else if let Some(id) = self.ids.tvdb { format!("tvdb_id={id}") }
            else { format!("imdb_id={}", self.ids.imdb.as_ref()?) };
        Some(format!("https://api.theintrodb.org/v3/media?{id}{}&duration_ms={}", self.address(), self.duration_ms))
    }
    fn fallback_url(&self) -> Option<String> {
        let movie = if self.episode.is_none() { "&is_movie=true" } else { "" };
        Some(format!("https://api.introdb.app/segments?imdb_id={}{}{movie}", self.ids.imdb.as_ref()?, self.address()))
    }
    fn matches_episode(&self, value: &Value) -> bool {
        match self.episode {
            Some((season, episode)) => value.get("season").and_then(Value::as_i64) == Some(season)
                && value.get("episode").and_then(Value::as_i64) == Some(episode),
            None => true,
        }
    }
}

fn public_json(url: &str) -> Option<Value> {
    // Fixed origins and validated query atoms only. No redirect, Authorization, Plex header,
    // client identifier or media data can cross this boundary.
    let timeout = plx_net::net::Timeouts { connect_s: 2, total_s: 0, total_ms: 3500, low_speed_bps: 0, low_speed_s: 0 };
    let response = plx_net::net::request(url, &[], "GET", None, timeout, false, Some(MAX_BODY), None)?;
    if response.status != 200 { return None; }
    serde_json::from_slice(&response.body).ok()
}

fn parse_primary(value: &Value, query: &Query) -> Option<Segments> {
    let expected_type = if query.episode.is_some() { "tv" } else { "movie" };
    if value.get("type")?.as_str()? != expected_type || !query.matches_episode(value) { return None; }
    let tmdb = value.get("tmdb_id")?.as_u64()?;
    if !(1..=10_000_000).contains(&tmdb) || query.ids.tmdb.is_some_and(|id| u64::from(id) != tmdb) { return None; }
    let mut result = Segments::default();
    for (index, key) in ["intro", "recap", "credits"].into_iter().enumerate() {
        let Some(rows) = value.get(key).and_then(Value::as_array) else { continue };
        if rows.is_empty() || rows.len() > 16 { continue; }
        let mut markers = Vec::new();
        let mut explicit_none = false;
        for row in rows {
            if no_segment(row, KINDS[index], true) { explicit_none = true; continue; }
            if let Some(marker) = segment(row, KINDS[index], query.duration_ms, true) { markers.push(marker); }
        }
        if !markers.is_empty() { markers.sort_by_key(|m| m.start_ms); result.0[index] = Some(markers); }
        else if explicit_none { result.0[index] = Some(Vec::new()); }
    }
    Some(result)
}

fn no_segment(row: &Value, kind: MarkerKind, primary: bool) -> bool {
    let zero_or_null = |name| row.get(name).is_some_and(|v| v.is_null() || v.as_i64() == Some(0));
    match kind {
        MarkerKind::Intro | MarkerKind::Recap => zero_or_null("start_ms") && zero_or_null("end_ms"),
        // TheIntroDB documents start=0 as no credits regardless of the end value. IntroDB's
        // independent wire contract uses the ordinary empty range instead.
        MarkerKind::Credits => row.get("start_ms").and_then(Value::as_i64) == Some(0)
            && (primary || zero_or_null("end_ms")),
    }
}

fn segment(row: &Value, kind: MarkerKind, duration: i64, nullable: bool) -> Option<Marker> {
    let start = match row.get("start_ms")? {
        Value::Null if nullable && kind != MarkerKind::Credits => 0,
        value => value.as_i64()?,
    };
    let end_value = row.get("end_ms")?;
    let end = match end_value {
        Value::Null if nullable && kind == MarkerKind::Credits => duration,
        value => value.as_i64()?,
    };
    // Never stretch a provider's interval or guess a terminal flag for a finite credits segment.
    // A finite end preserves a following scene; null end (or exact media end) is terminal.
    if start < 0 || end <= start || end > duration || end > MAX_TIME_MS { return None; }
    Some(Marker { kind, start_ms: start, end_ms: end,
        final_seg: kind == MarkerKind::Credits && end == duration })
}

fn parse_fallback(value: &Value, query: &Query) -> Option<Segments> {
    if value.get("imdb_id")?.as_str()? != query.ids.imdb.as_deref()? || !query.matches_episode(value) { return None; }
    let expected_type = if query.episode.is_some() { "tv" } else { "movie" };
    if value.get("media_type").and_then(Value::as_str).is_some_and(|kind| kind != expected_type) { return None; }
    let mut result = Segments::default();
    let post_scene = value.get("post_credits").and_then(|row| segment(row, MarkerKind::Credits, query.duration_ms, false));
    // Even when the provider knows only the scene and not the credits before it, do not let
    // the guessed 30-second tail cut that scene off with an Up Next countdown.
    result.1 = post_scene.is_some();
    for (index, key) in ["intro", "recap", "outro"].into_iter().enumerate() {
        let Some(row) = value.get(key).filter(|v| v.is_object()) else { continue };
        if no_segment(row, KINDS[index], false) { result.0[index] = Some(Vec::new()); continue; }
        if let Some(mut marker) = segment(row, KINDS[index], query.duration_ms, false) {
            if marker.kind == MarkerKind::Credits {
                // IntroDB has a separate after-credits scene. Never finish over one, including
                // a contradictory outro ending at duration: the after-credits scene wins.
                if let Some(scene_start) = post_scene.as_ref().map(|scene| scene.start_ms) {
                    if scene_start >= marker.start_ms && scene_start < query.duration_ms {
                        marker.end_ms = marker.end_ms.min(scene_start);
                        marker.final_seg = false;
                    }
                }
            }
            if marker.end_ms > marker.start_ms { result.0[index] = Some(vec![marker]); }
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query() -> Query {
        Query { ids: Ids { tmdb: Some(1396), imdb: Some("tt0903747".into()), ..Default::default() },
            episode: Some((2, 3)), duration_ms: 2_700_000 }
    }
    fn primary(extra: Value) -> Value {
        let mut value = json!({"tmdb_id":1396, "type":"tv", "season":2, "episode":3});
        for (key, field) in extra.as_object().unwrap() { value[key] = field.clone(); }
        value
    }
    fn fallback(extra: Value) -> Value {
        let mut value = json!({"imdb_id":"tt0903747", "media_type":"tv", "season":2, "episode":3});
        for (key, field) in extra.as_object().unwrap() { value[key] = field.clone(); }
        value
    }

    #[test]
    fn external_ids_are_exact_validated_entities_and_queries_include_release_duration() {
        let metadata: plex::Metadata = serde_json::from_value(json!({
            "guid":"plex://show/xyz", "Guid":[{"id":"imdb://tt0903747"}, {"id":"tmdb://1396"},
                {"id":"tvdb://81189"}, {"id":"tmdb://9&token=private"}]
        })).unwrap();
        let ids = Ids::from_metadata(&metadata);
        assert_eq!((ids.tmdb, ids.tvdb, ids.imdb.as_deref()), (Some(1396), Some(81189), Some("tt0903747")));
        assert_eq!(query().primary_url().as_deref(), Some("https://api.theintrodb.org/v3/media?tmdb_id=1396&season=2&episode=3&duration_ms=2700000"));
        assert_eq!(query().fallback_url().as_deref(), Some("https://api.introdb.app/segments?imdb_id=tt0903747&season=2&episode=3"));
        let missing: plex::Metadata = serde_json::from_value(json!({"guid":"plex://episode/xyz", "Guid":null})).unwrap();
        assert!(Query { ids: Ids::from_metadata(&missing), ..query() }.primary_url().is_none());
        for malformed in [json!(7), json!([null, {"id":7}, {"id":"tmdb://1396"}])] {
            let metadata: plex::Metadata = serde_json::from_value(json!({"Guid":malformed})).unwrap();
            assert!(metadata.guid.is_empty(), "optional provider identity cannot break the Plex item");
        }
        assert!(!valid_imdb("tt0903747&X-Plex-Token=secret"));
        assert!(numeric_id("10000001").is_none());
    }

    #[test]
    fn nullable_primary_times_and_multiple_credits_preserve_the_scene_between_them() {
        let value = primary(json!({
            "intro":[{"start_ms":null,"end_ms":23000}],
            "recap":[{"start_ms":null,"end_ms":7000}],
            "credits":[{"start_ms":2500000,"end_ms":2600000}, {"start_ms":2650000,"end_ms":null}]
        }));
        let segments = parse_primary(&value, &query()).unwrap();
        assert_eq!(segments.0[0].as_ref().unwrap()[0].start_ms, 0);
        assert_eq!(segments.0[1].as_ref().unwrap()[0].kind, MarkerKind::Recap);
        let credits = segments.0[2].as_ref().unwrap();
        assert_eq!(credits.len(), 2);
        assert!(!credits[0].final_seg);
        assert!(credits[1].final_seg);
        assert_eq!(credits[1].end_ms, query().duration_ms);
        assert!(super::super::marker_at(credits, 2_625_000).is_none(), "after-credits scene is not skipped");
    }

    #[test]
    fn explicit_no_segment_blocks_secondary_fallback_but_missing_kinds_are_filled() {
        let mut segments = parse_primary(&primary(json!({"intro":[{"start_ms":0,"end_ms":0}],
            "credits":[{"start_ms":0,"end_ms":null}]})), &query()).unwrap();
        let fallback = parse_fallback(&fallback(json!({"intro":{"start_ms":10000,"end_ms":90000},
            "recap":{"start_ms":0,"end_ms":15000}, "outro":{"start_ms":2600000,"end_ms":2700000}})), &query()).unwrap();
        segments.fill_missing(fallback);
        assert!(segments.0[0].as_ref().unwrap().is_empty());
        assert!(segments.0[1].as_ref().is_some_and(|s| s.len() == 1));
        assert!(segments.0[2].as_ref().unwrap().is_empty());
        let no_credits = parse_primary(&primary(json!({"credits":[{"start_ms":0,"end_ms":2600000}]})), &query()).unwrap();
        assert!(no_credits.0[2].as_ref().unwrap().is_empty(), "zero credits start is authoritative even with a positive end");
    }

    #[test]
    fn an_after_credits_scene_without_known_outro_suppresses_the_guessed_tail() {
        let segments = parse_fallback(&fallback(json!({"outro":null,
            "post_credits":{"start_ms":2680000,"end_ms":2700000}})), &query()).unwrap();
        assert!(segments.0[2].is_none(), "no invented credits interval");
        let answer = Answer { request: Request { generation: 0, capture: capture("one") }, segments };
        assert!(answer.no_credits(), "the synthetic tail must not auto-advance over the known scene");
        let invalid = parse_fallback(&fallback(json!({"post_credits":{"start_ms":2700001,"end_ms":2800000}})), &query()).unwrap();
        assert!(!invalid.1, "an invalid scene cannot affect playback");
    }

    #[test]
    fn canonical_episode_lookup_requires_a_positive_tmdb_aired_order_and_show_identity() {
        let show: plex::Metadata = serde_json::from_value(json!({"type":"show",
            "showOrdering":"tmdbAiring", "Guid":[{"id":"tmdb://1396"}, {"id":"imdb://tt0903747"}]})).unwrap();
        assert!(canonical_episode_ids(&show, Some(show.show_ordering.as_str())).is_some());
        // A resolved library default is the same canonical source as an explicit show override.
        assert!(canonical_episode_ids(&show, Some("tmdbAiring")).is_some());
        for unsupported in [None, Some(""), Some("dvd"), Some("absolute"), Some("aired"), Some("unknown")] {
            assert!(canonical_episode_ids(&show, unsupported).is_none());
        }
        let episode_only: plex::Metadata = serde_json::from_value(json!({"type":"episode",
            "Guid":[{"id":"tmdb://99999"}, {"id":"imdb://tt0999999"}]})).unwrap();
        assert!(canonical_episode_ids(&episode_only, Some("tmdbAiring")).is_none(), "an episode IMDb ID cannot stand in for its show's canonical identity");
    }

    #[test]
    fn provider_markers_fill_missing_kinds_without_replacing_plex_or_retiring_post_credit_scenes() {
        let mut markers = vec![Marker { kind: MarkerKind::Intro, start_ms: 1000, end_ms: 2000, final_seg: false }];
        let segments = parse_fallback(&fallback(json!({"intro":{"start_ms":0,"end_ms":90000},
            "recap":{"start_ms":2000,"end_ms":10000}, "outro":{"start_ms":2600000,"end_ms":2700000},
            "post_credits":{"start_ms":2680000,"end_ms":2700000}})), &query()).unwrap();
        assert!(segments.merge_into(&mut markers));
        assert_eq!((markers[0].start_ms, markers[0].end_ms), (1000, 2000));
        assert_eq!(markers.len(), 3);
        assert!(!markers[2].final_seg);
        assert_eq!(markers[2].end_ms, 2_680_000);
        assert!(!segments.merge_into(&mut markers), "a repeated answer never duplicates a kind");
    }

    #[test]
    fn malformed_out_of_range_or_wrong_episode_answers_cannot_offer_a_seek() {
        for row in [json!({"start_ms":-1,"end_ms":1000}), json!({"start_ms":3000,"end_ms":2000}),
            json!({"start_ms":0,"end_ms":2700001}), json!({"start_ms":0,"end_ms":1.5}),
            json!({"start_ms":0,"end_ms":9223372036854775807_i64})] {
            let segments = parse_primary(&primary(json!({"intro":[row]})), &query()).unwrap();
            assert!(segments.0[0].is_none());
        }
        let mut wrong = primary(json!({"intro":[{"start_ms":0,"end_ms":1000}]}));
        wrong["episode"] = json!(4);
        assert!(parse_primary(&wrong, &query()).is_none());
        wrong["episode"] = json!(3);
        wrong["tmdb_id"] = json!(999);
        assert!(parse_primary(&wrong, &query()).is_none());
        let mut wrong = fallback(json!({}));
        wrong["imdb_id"] = json!("tt0000001");
        assert!(parse_fallback(&wrong, &query()).is_none());
    }

    fn capture(rk: &str) -> Capture {
        Capture { sid: ServerId::from_raw(1), rk: rk.into(), machine: "test-machine".into(),
            client: 1, token: 2, profile: 3, user: "profile".into(), duration_ms: 2700000 }
    }

    #[test]
    fn cancellation_discards_pending_and_landed_old_episode_data_and_cache_keys_scope_identity() {
        let worker = Worker::default();
        let request = Request { generation: 0, capture: capture("episode-one") };
        {
            let mut state = worker.state();
            state.pending = Some(request.clone());
            state.answer = Some(Answer { request, segments: Segments::default() });
            state.cache.push_back(Cached { capture: capture("episode-one"), expires: Instant::now() + RESULT_TTL,
                segments: Segments::default() });
        }
        worker.cancel();
        assert!(worker.state().pending.is_none());
        assert!(worker.state().answer.is_none());
        assert_eq!(worker.state().generation, 1);
        let late = Request { generation: 0, capture: capture("episode-one") };
        assert!(!worker.state().publish(late, Segments::default(), Instant::now(), true));
        assert!(worker.state().answer.is_none(), "a late old completion cannot repopulate the landing");
        assert!(worker.cached(&capture("episode-one"), Instant::now()).is_some(), "negative result is cached too");
        for changed in [Capture { rk: "episode-two".into(), ..capture("episode-one") },
            Capture { sid: ServerId::from_raw(2), ..capture("episode-one") },
            Capture { profile: 4, ..capture("episode-one") },
            Capture { token: 4, ..capture("episode-one") },
            Capture { duration_ms: 2800000, ..capture("episode-one") }] {
            assert!(worker.cached(&changed, Instant::now()).is_none());
        }
        assert!(worker.cached(&capture("episode-one"), Instant::now() + RESULT_TTL).is_none());
    }
}
