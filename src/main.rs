// Zotify genre tagger
// Ari Rios <me@aririos.com>
// License: MIT
//!
//! For when you forgot to enable genre tagging in Zotify.
#![feature(closure_lifetime_binder)]

use anyhow::Result;
use dotenvy;
use ffmpeg_next::{
    Rational, Stream, codec, encoder,
    format::{self, context::Input},
    media,
};
use futures::future::join_all;
use log::{debug, error, info, trace};
use rspotify::{
    ClientCredsSpotify, Credentials,
    model::{ArtistId, TrackId},
    prelude::*,
};
use std::{env, time::Duration};
use std::fs::{self, DirEntry};
use std::io::Error;
use std::path::PathBuf;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
};
use tokio;
use rand::Rng;

/// ContextOrStream is used to abstract over metadata assigned to a container 
///  or to a specific stream inside that container.
enum ContextOrStream<'a> {
    Context(&'a Input),
    Stream(&'a Stream<'a>),
}

/// insert_song_path will insert a [PathBuf] matching a given [TrackId] into paths_by_track_id.
/// `id` is the TrackId as a [String].
/// `song_result_wrapped` is the [Result] of the song file search.
/// `found_counter`, `dup_counter`, and `error_counter` are references to success, duplicate, and error counters.
/// `paths_by_track_id` is passed directly.
/// `album_folder` is the [Result] of the album folder search.
fn insert_song_path(
    id: String,
    song_result_wrapped: &Result<DirEntry, Error>,
    found_counter: &mut i32,
    dup_counter: &mut i32,
    error_counter: &mut i32,
    paths_by_track_id: Arc<Mutex<HashMap<TrackId, PathBuf>>>,
    album_folder: &Vec<Result<DirEntry, Error>>,
) -> Result<()> {
    trace!(
        "insert_song_path(id: {id:?}, song_result_wrapped: {song_result_wrapped:?}, found_counter: {found_counter}, dup_counter: {dup_counter}, error_counter: {error_counter}, paths_by_track_id: {paths_by_track_id:?}, album_folder: {album_folder:?})"
    );
    match song_result_wrapped {
        Ok(song_result) => {
            *found_counter += 1;
            let prev_value = paths_by_track_id.lock().unwrap().insert(
                TrackId::from_id(id.clone())?,
                song_result.path(),
            );
            if let Some(prev_value) = prev_value {
                *dup_counter += 1;
                let key = &TrackId::from_id(&id)?;
                match paths_by_track_id.lock().unwrap().get(key) {
                    Some(entry) => {
                        debug!("prev_value for {} was {:?}", entry.display(), prev_value);
                    } 
                    None => {
                        debug!("prev_value for {} was {:?}", key, prev_value);
                    }
                }
            }
        }
        Err(e) => {
            *error_counter += 1;
            error!("Error on retrieving song path at album_folder {album_folder:?}: {e}");
        }
    }

    Ok(())
}

/// chunk_hashmap partitions a [HashMap] into `N` chunks, with the remainder in the final chunk.
/// The type generics `U` and `V` are the types of HashMap's keys and values, respectively.
/// `map` is the HashMap to chunk.
/// `total_len` is the total length of the HashMap if chunking should be based on something other than `map.len()`
/// (such as if the values are [Vec]s), otherwise None.
/// `map_values` is a closure that is passed to [Iterator::flat_map] on the Vec<(U, V)> representation of the HashMap
/// before chunking occurs if the values need to be remapped somehow, such as if, again, the values are [Vec]s,
/// and you want the chunks to flatten those Vecs; otherwise, pass None::<fn(&(U, V)) -> Vec<(U, V)>>.
fn chunk_hashmap<const N: usize, U: Clone, V: Clone>(
    map: HashMap<U, V>,
    total_len: Option<usize>,
    map_values: Option<impl FnMut(&(U, V)) -> Vec<(U, V)>>
) -> Vec<Vec<(U, V)>> {
    let len = total_len.unwrap_or(map.len());
    let num_chunks = (len as f64 / N as f64).ceil() as usize;
    let mut iter_as_vec = map.into_iter().collect::<Vec<(U, V)>>();
    if let Some(value_mapper) = map_values {
        iter_as_vec = iter_as_vec.iter().flat_map(value_mapper).collect::<Vec<(U, V)>>();
    }
    let iter_as_chunks: (&[[(U, V); N]], &[(U, V)]) = iter_as_vec.as_chunks::<N>();
    (0..num_chunks)
        .map(|i| {
            if num_chunks == 1 {
                if len < N {
                    iter_as_chunks.1.to_vec()
                } else {
                    iter_as_chunks.0[i].to_vec()
                }
            } else {
                if i < num_chunks - 1 {
                    iter_as_chunks.0[i].to_vec()
                } else {
                    iter_as_chunks.1.to_vec()
                }
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    // Handle background panics in threads or futures
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    env_logger::init();
    dotenvy::dotenv()?;

    let base_path = env::var("BASE_PATH")?;
    println!("Getting folders in {base_path}");
    let paths_by_track_id: Arc<Mutex<HashMap<TrackId<'_>, PathBuf>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let all_songs: Vec<_> = fs::read_dir(base_path)?
        .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_dir())
        .flat_map(|artist_folder| fs::read_dir(artist_folder.as_ref().unwrap().path()))
        .flatten()
        .flat_map(|album_folder| fs::read_dir(album_folder.unwrap().path()))
        .map(|album_folder| album_folder.collect::<Vec<_>>())
        .collect();

    let mut found_counter = 0;
    let mut not_found_counter = 0;
    let mut error_counter = 0;
    let mut dup_counter = 0;

    println!("Processing folders...");
    for album_folder in all_songs {
        let song_ids_file = album_folder
            .iter()
            .find(|entry| entry.as_ref().unwrap().file_name() == ".song_ids");
        if let Some(file) = song_ids_file {
            let song_ids_str = fs::read_to_string(file.as_ref().unwrap().path())?;
            let song_ids: Vec<Vec<String>> = if !song_ids_str.is_empty() {
                song_ids_str
                    .lines()
                    .map(|line| line.split('\t').map(|s| s.to_owned()).collect::<Vec<_>>())
                    .collect()
            } else {
                continue;
            };
            for id in song_ids {
                let song = album_folder
                    .iter()
                    .find(|entry| *entry.as_ref().unwrap().file_name() == **id.get(4).unwrap());
                match song {
                    Some(song_result_wrapped) => {
                        insert_song_path(
                            id.get(0).unwrap().to_string(),
                            song_result_wrapped,
                            &mut found_counter,
                            &mut dup_counter,
                            &mut error_counter,
                            Arc::clone(&paths_by_track_id),
                            &album_folder,
                        )?;
                    }
                    None => {
                        // Try again with base_path prefix
                        let song = album_folder.iter().find(|entry| {
                            *entry.as_ref().unwrap().path().as_os_str() == **id.get(4).unwrap()
                        });
                        match song {
                            Some(song_result_wrapped) => {
                                insert_song_path(
                                    id.get(0).unwrap().to_string(),
                                    song_result_wrapped,
                                    &mut found_counter,
                                    &mut dup_counter,
                                    &mut error_counter,
                                    Arc::clone(&paths_by_track_id),
                                    &album_folder,
                                )?;
                            }
                            None => {
                                not_found_counter += 1;
                                error!("No song found matching song_id at {id:?}");
                            }
                        }
                    }
                }
            }
        } else {
            error!(
                "No .song_ids file found for album folder {:?}",
                album_folder
            )
        }
    }

    println!("Tracks found successfully: {found_counter}");
    println!("Tracks not found: {not_found_counter}");
    println!("Duplicates: {dup_counter}");
    println!("Errors: {error_counter}");

    println!("Grabbing genres from Spotify...");
    let spotify_creds = Credentials::from_env().unwrap();

    let spotify = Arc::new(ClientCredsSpotify::new(spotify_creds));

    spotify.request_token().await.unwrap();

    let genres_by_artist: Arc<Mutex<HashMap<ArtistId, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let genres_by_track: Arc<Mutex<HashMap<TrackId, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut genre_tasks = vec![];

    const CHUNK_SIZE: usize = 50;
    let path_chunks = chunk_hashmap::<CHUNK_SIZE, TrackId, PathBuf>(
        paths_by_track_id.lock().unwrap().clone(),
        None,
        None::<for <'a, 'b> fn(&'a (TrackId<'b>, PathBuf)) -> Vec<(TrackId<'b>, PathBuf)>>
    );
    debug!("path_chunks: {path_chunks:?}");
    let mut i = 0;
    for path_chunk in path_chunks {
        i += 1;
        if path_chunk.len() > 0 {
            let spotify = spotify.clone();
            let genres_by_artist = Arc::clone(&genres_by_artist);
            let genres_by_track = Arc::clone(&genres_by_track);
            let num_paths = paths_by_track_id.lock().unwrap().len() as u64;
            genre_tasks.push(tokio::spawn(async move {
                // Try to prevent 429s
                let rand_millis = rand::rng().random_range(0..(num_paths * 10));
                tokio::time::sleep(Duration::from_millis(rand_millis)).await;
                
                let res = spotify
                .tracks(
                    path_chunk.into_iter().map(|(track, _)| track.clone()),
                    None,
                )
                .await.unwrap();
                let mut artists_by_track: HashMap<TrackId, Vec<ArtistId>> = HashMap::new();
                for track in res {
                    let id = track.id.unwrap();
                    let artists = track.artists.clone();
                    artists_by_track.insert(
                        id,
                        artists
                            .into_iter()
                            .map(|artist| artist.id.as_ref().unwrap().to_owned())
                            .collect(),
                    );
                }
                debug!("artists_by_track {i}: {artists_by_track:?}");
                let mut artists_by_track_orig = artists_by_track.clone();
                let artists_len = artists_by_track.iter().fold(0, |acc, (_, artists)| acc + artists.len());
                let artist_chunks: Vec<Vec<(TrackId<'_>, Vec<ArtistId<'_>>)>> = chunk_hashmap::<CHUNK_SIZE, TrackId, Vec<ArtistId>>(
                    artists_by_track, 
                    Some(artists_len),
                    Some(Box::new(for <'a, 'b, 'c>
                        |(track, artists): &'a (TrackId<'b>, Vec<ArtistId<'c>>)| -> Vec<(TrackId<'b>, Vec<ArtistId<'c>>)> {
                            artists.into_iter().map(|artist|
                                (track.clone(), std::iter::once(artist.clone()).collect()))
                                .collect()
                            })
                        )
                );
                let artist_chunks: Vec<Vec<Vec<ArtistId<'_>>>> = artist_chunks.into_iter().map(|chunk| chunk.into_iter().map(|(_, artists)| artists).collect()).collect();
                debug!("artist_chunks {i}: {artist_chunks:?}");
                for artist_chunk in artist_chunks {
                    if artist_chunk.len() > 0 {
                        let res = spotify.artists(artist_chunk.into_iter().flatten().collect::<Vec<ArtistId>>()).await.unwrap();
                        for artist in res {
                            genres_by_artist.lock().unwrap().insert(artist.id, artist.genres);
                        }
                    }
                }
                debug!("genres_by_artist {i}: {genres_by_artist:?}");
                for (artist, genres) in genres_by_artist.lock().unwrap().iter() {
                    debug!("artist {i}: {artist:?}");
                    artists_by_track_orig.retain(|track, artists| {
                        debug!("artists {i}: {artists:?}");
                        if artists.contains(&artist) {
                            genres_by_track
                                .lock()
                                .unwrap()
                                .entry(track.clone())
                                .and_modify(|existing_genres| existing_genres.append(&mut genres.clone()))
                                .or_insert(genres.clone());
                            if artists.len() == 1 {
                                debug!("{i}: removed");
                                false
                            } else if artists.len() > 1 {
                                let artist_idx = artists.iter().position(|art| *art == *artist);
                                artists.remove(artist_idx.unwrap());
                                debug!("{i}: decremented");
                                true
                            } else {
                                error!("Unknown state in artist_by_track_orig.retain: artist: {artist:?}, genres_by_track: {genres_by_track:?}");
                                false
                            }
                        }
                        else {
                            debug!("{i}: skipped");
                            true
                        }
                    });
                }
                if artists_by_track_orig.len() != 0 {
                    error!("Artists without matching tracks {i}: {artists_by_track_orig:?}");
                }
            }));
        }
    }

    join_all(genre_tasks).await;

    for (_track, genres) in genres_by_track.lock().unwrap().iter_mut() {
        genres.sort();
        genres.dedup();
    }

    debug!("genres_by_track: {genres_by_track:?}");

    println!("Writing genres to disk...");

    ffmpeg_next::init()?;

    let genres_lock = genres_by_track.lock().unwrap();
    thread::scope(|scope| {
        for (track, genres) in genres_lock.iter() {
            scope.spawn(|| {
                let paths = paths_by_track_id.lock().unwrap();
                let path = paths.get(track).unwrap();
                info!("Processing file {}", path.display());
                let mut ictx = format::input(path).unwrap();
                let context_or_stream = if ictx.metadata().iter().count() != 0 {
                    ContextOrStream::Context(&ictx)
                } else {
                    ContextOrStream::Stream(&ictx.streams().best(media::Type::Audio).unwrap())
                };
                let mut temp_path = path.clone();
                temp_path.set_extension(
                    path.extension().unwrap().to_string_lossy().into_owned() + ".tmp",
                );
                let mut octx = format::output_as(&temp_path, "ogg").unwrap();
                let mut stream_mapping: Vec<i32> = vec![0; ictx.nb_streams() as _];
                let mut ist_time_bases = vec![Rational(0, 1); ictx.nb_streams() as _];
                let mut ost_index = 0;
                for (ist_index, ist) in ictx.streams().enumerate() {
                    let ist_medium = ist.parameters().medium();
                    if ist_medium != media::Type::Audio {
                        stream_mapping[ist_index] = -1;
                        continue;
                    }
                    stream_mapping[ist_index] = ost_index;
                    ist_time_bases[ist_index] = ist.time_base();
                    ost_index += 1;
                    let mut ost = octx.add_stream(encoder::find(codec::Id::OPUS)).unwrap();
                    ost.set_parameters(ist.parameters());
                    unsafe {
                        (*ost.parameters().as_mut_ptr()).codec_tag = 0;
                    }
                }
                match context_or_stream {
                    ContextOrStream::Context(ictx) => {
                        let mut octx_metadata = ictx.metadata().to_owned();
                        octx_metadata.set("genre", &genres.join(","));
                        octx.set_metadata(octx_metadata);
                    }
                    ContextOrStream::Stream(input) => {
                        let mut output = octx
                            .streams_mut()
                            .find(|s| {
                                codec::context::Context::from_parameters(s.parameters())
                                    .unwrap()
                                    .medium()
                                    == media::Type::Audio
                            })
                            .unwrap();
                        let mut output_metadata = input.metadata().to_owned();
                        output_metadata.set("genre", &genres.join(","));
                        output.set_metadata(output_metadata);
                    }
                }

                octx.write_header().unwrap();

                for (stream, mut packet) in ictx.packets() {
                    let ist_index = stream.index();
                    let ost_index = stream_mapping[ist_index];
                    if ost_index < 0 {
                        continue;
                    }
                    let ost = octx.stream(ost_index as _).unwrap();
                    packet.rescale_ts(ist_time_bases[ist_index], ost.time_base());
                    packet.set_position(-1);
                    packet.set_stream(ost_index as _);
                    packet.write_interleaved(&mut octx).unwrap();
                }

                octx.write_trailer().unwrap();

                fs::remove_file(path).unwrap();
                fs::rename(temp_path, path).unwrap();
            });
        }
    });

    println!("Finished!");

    Ok(())
}
