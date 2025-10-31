# zotify-genre-tagger
For when you forgot to enable genre tagging in Zotify.
This crate will run through a given folder and use the `.song_ids` files that Zotify leaves
in each album's directory to look up your songs' artists' genres on Spotify and tag each file appropriately.
Spotify only currently assigns genres to artists, not individual songs. This is the same thing Zotify would do
with both the `MD_SAVE_GENRES` and `MD_ALLGENRES` options enabled and `MD_GENREDELIMITER` set to ",".

# Configuation
You will need to create a Spotify API key at the [Spotify for Developers Dashboard](https://developer.spotify.com/dashboard).

Use a .env file or pass as environment variables:
```
RSPOTIFY_CLIENT_ID={your Spotify client ID}
RSPOTIFY_CLIENT_SECRET={your Spotify client secret}
BASE_PATH={wherever you pointed Zotify at}
```

Currently only takes as input `.ogg` files (which is what Spotify uses natively anyway) and only outputs `.ogg` files (with an Opus encoding to save space). This can be changed in the ffmpeg remuxing section at the bottom of main.rs.

# Usage
Then, just run `zotify-genre-tagger`.

# Building
This uses a nightly Rust feature, #![feature(closure_lifetime_binder)], so you'll have to download it with `rustup toolchain install nightly` and then switch to it for this project with `rustup override set nightly`.
