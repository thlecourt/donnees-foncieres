-------------
---Ce script crée une table unique des contraintes à la mobilisattion du foncier
------------

DROP TABLE IF EXISTS ign_topo.contraintes_jointes CASCADE;

CREATE TABLE ign_topo.contraintes_jointes AS (
	SELECT 'basol' as source,
		gid::text as id_source,
		'pollué' as categorie,
		'pollué' as nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM nat.pollut
	UNION
	SELECT 'aerodrome_cut_2021' as source,
		id::text as id_source,
		'aérodrome' as categorie,
		'aérodrome' as nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.aerodrome_cut_2021
	UNION
	SELECT 'cimetiere_2021' as source,
		id::text as id_source,
		'cimetière' as categorie,
		'cimetière' as nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.cimetiere_2021
	UNION
	SELECT 'equipement_de_transport_2021' as source,
		id::text as id_source,
		categorie,
		nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.equipement_de_transport_2021
	WHERE nature NOT LIKE 'Station de métro'
		AND nature NOT LIKE 'Service dédié%%'
	UNION
	SELECT 'piste_d_aerodrome_cut_2021' as source,
		id::text as id_source,
		'aérodrome' as categorie,
		'piste' as nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.piste_d_aerodrome_cut_2021
	UNION
	SELECT 'terrain_de_sport_2021' as source,
		id::text as id_source,
		categorie,
		nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.terrain_de_sport_2021
	UNION
	SELECT 'toponymie_transport_2021' as source,
		id::text as id_source,
		categorie,
		nature,
		Null as geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.toponymie_transport_2021
	WHERE nature NOT LIKE 'Station de métro'
		AND nature NOT LIKE 'Service dédié%%'
	UNION
	SELECT 'transport_par_cable_2021' as source,
		id::text as id_source,
		categorie,
		nature,
		Null as geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.transport_par_cable_2021
	UNION
	SELECT 'zai_cut_2021' as source,
		id::text as id_source,
		categorie,
		nature,
		geom_poly,
		geom_point,
		idcom22,
		iddep22
	FROM ign_topo.zai_cut_2021
	WHERE categorie NOT LIKE 'Autre'
						AND nature NOT LIKE 'Abri%%'
						AND nature NOT LIKE 'Point%%'
						AND nature NOT LIKE 'Refuge'
						AND nature NOT LIKE 'Sentier%%'
						AND nature NOT LIKE 'Borne%%'
);

CREATE INDEX ign_topo_contraintes_jointes_geom_poly_idx ON ign_topo.contraintes_jointes USING gist(geom_poly);
CREATE INDEX ign_topo_contraintes_jointes_geom_point_idx ON ign_topo.contraintes_jointes USING gist(geom_point);
CREATE INDEX ign_topo_contraintes_jointes_iddep22_idcom22_idx ON ign_topo.contraintes_jointes(iddep22,idcom22);
