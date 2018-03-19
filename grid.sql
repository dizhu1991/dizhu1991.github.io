
-- create table of mesh grids for AUS
DROP TABLE IF EXISTS au_grid_table_4326;
CREATE TABLE au_grid_table_4326 (id varchar(30),gid serial PRIMARY KEY, geom geometry(POLYGON,4326));

INSERT INTO au_grid_table_4326(id, geom)
    SELECT concat('GDAU','_',hor.n,'_',ver.n) -- the formation of a grid's id will be something like A_1_5, the first number being its position on the E-W axis, the second being its position on the N-S axis.
	AS id
    , ST_Translate(ST_MakeEnvelope(108 - 0.01, -45 - 0.01, 108, -45, 4326),
    hor.n*0.01, ver.n*0.01) As geom -- 108 and -45 is the lon and lat of the most Southwestern corner of the mesh. Every grid is 0.1degree * 0.1 degree.
    FROM
    generate_series(1,4700) as hor(n), generate_series(1,3700) as ver(n); -- generating a 470*370 mesh.

CREATE INDEX au_idx_grid_test2_4326 ON au_grid_table_4326 USING GIST(geom);

ALTER TABLE public.au_grid_table_4326 ADD grid_area NUMERIC NULL;
UPDATE au_grid_table_4326 SET grid_area=1; -- 1 km^2.



-- create table of mesh grids for NZL
DROP TABLE IF EXISTS nz_grid_table_4326;
CREATE TABLE nz_grid_table_4326 (id varchar(30),gid serial PRIMARY KEY, geom geometry(POLYGON,4326));

INSERT INTO nz_grid_table_4326(id, geom)
    SELECT concat('GDNZ','_',hor.n,'_',ver.n) AS id
    , ST_Translate(ST_MakeEnvelope(166.33 - 0.01, -47.4 - 0.01, 166.33, -47.4, 4326),
    hor.n*0.01, ver.n*0.01) As geom -- 166.33, -47.4 is the lon and lat of the most Southwestern corner in the mesh, Every grid is 0.05*0.05 square degrees.
    FROM
    generate_series(1,256*5) as hor(n), generate_series(1,264*5) as ver(n); -- generating a 256*264 mesh.

CREATE INDEX idx_grid_table_geom_4326 ON nz_grid_table_4326 USING GIST(geom);

ALTER TABLE public.nz_grid_table_4326 ADD grid_area NUMERIC NULL; -- add the column of grid_area
UPDATE nz_grid_table_4326 SET grid_area=1;  -- 1 km^2.



-- create the table of events_by_grid using events on Nov 1, 2017
DROP TABLE IF EXISTS events_by_grid;
CREATE TABLE events_by_grid AS
(SELECT odg.total_count,  --total event count in every grid
	odg.grid_id,    -- grid id
   st_x(st_centroid(a.geom)) lon, -- lon and lat of the centroid of a grid
   st_y(st_centroid(a.geom)) lat,
   a.grid_area     -- area of the grid
FROM
(SELECT count(bbb.geom) total_count, bbb.id grid_id from
(select a.tourism_region_name, a.geom, b.id from
	(select * from dradis_events_201711 where created_at::DATE = '2017-11-01' and country_code='nz'
) a, nz_grid_table_4326 b
WHERE st_intersects(a.geom, b.geom)) bbb
GROUP BY bbb.id) odg, nz_grid_table_4326 a
WHERE odg.grid_id = a.id)
;


select * FROM events_by_grid;

select count(*) from dradis_events_201711 where created_at::DATE = '2017-11-01' and country_code='nz';

SELECT sum(nz.total_count) from events_by_grid nz;