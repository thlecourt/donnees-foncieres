-------------------------
---Ce script détermine la localisation des parcelles selon la géographie communale et départementale au 1er janvier 2022
-------------------------

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	table_name_new text;

BEGIN

FOREACH millesime IN ARRAY ARRAY['2009','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
LOOP
  RAISE NOTICE 'Traitement du millésime % en cours',millesime;
  millesime_short = RIGHT(millesime,2);
  table_name_new = 'public'||millesime_short;

RAISE NOTICE 'Recherche idcom22, iddep22 et indexation';
	
	EXECUTE format(
		$$
		

		CREATE INDEX nat_%1$s_idpar_idx ON nat.%1$s(idpar);
		CREATE INDEX nat_%1$s_geomloc_idx ON nat.%1$s USING gist(geomloc);
		CREATE INDEX nat_%1$s_geompar_idx ON nat.%1$s USING gist(geompar);

		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS idcom22 CASCADE;
		ALTER TABLE nat.%1$s ADD COLUMN idcom22 varchar(5);
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS iddep22 CASCADE;
		ALTER TABLE nat.%1$s ADD COLUMN iddep22 varchar(3);

		UPDATE nat.%1$s a
		SET idcom22 =
			CASE
				WHEN idcom LIKE '132%%' THEN '13055'
				WHEN idcom LIKE '75%%' THEN '75056'
				WHEN idcom LIKE '6938%%' THEN '69123'
			END,
			iddep22 = 
			CASE
				WHEN idcom LIKE '132%%' THEN '13'
				WHEN idcom LIKE '75%%' THEN '75'
				WHEN idcom LIKE '6938%%' THEN '69'
			END
		WHERE idcom LIKE '132%%'
			OR idcom LIKE '75%%'
			OR idcom LIKE '6938%%';

		UPDATE nat.%1$s a
		SET idcom22 = b.codgeo_2022,
			iddep22 = c.insee_dep
		FROM insee.table_passage_idcom22 b, insee.geocom22 c
		WHERE a.idcom22 IS NULL
			AND a.idcom = b.codgeo_ini
			AND b.codgeo_2022 = c.insee_com;			

		CREATE INDEX nat_%1$s_idcom22_idx ON nat.%1$s(idcom22);
		CREATE INDEX nat_%1$s_iddep22_idx ON nat.%1$s(iddep22);
		$$,
		table_name_new);
	
	COMMIT;
END LOOP;
END
$do$
