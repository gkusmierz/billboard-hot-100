select
    a.date,
    b.artist,
    b.song,
    b.trackId
from
    hot_ones a
left outer join
    artist_songs b on a.songId = b.id
where date('1977-06-22') between a.date and date(a.date,'6 day')