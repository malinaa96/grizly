SELECT sq1.PlaylistId AS PlaylistId,
       sq1.TrackId AS TrackId,
       sq1.Name AS Name
FROM
  (SELECT sq1.PlaylistId AS PlaylistId,
          sq1.TrackId AS TrackId,
          sq2.Name AS Name
   FROM
     (SELECT PlaylistId,
             TrackId
      FROM playlist_track) sq1
   CROSS JOIN
     (SELECT PlaylistId,
             Name
      FROM playlists) sq2) sq1
RIGHT JOIN
  (SELECT PlaylistId,
          TrackId
   FROM playlist_track) sq2 ON sq1.PlaylistId=sq2.PlaylistId
FULL JOIN
  (SELECT PlaylistId,
          Name
   FROM playlists) sq3 ON sq2.PlaylistId=sq3.PlaylistId