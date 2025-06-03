----------
--- Ce script crée une table des bâtiments apparus entre 2016 et 2021
----------

---Création de la table
CREATE TABLE nat.nouvelles_constructions_16_21 AS (
	SELECT groupe.*, usage.usage_principal_bdnb_open
	FROM bdnb_2024_10_a_open_data.batiment_groupe groupe
	LEFT JOIN bdnb_2024_10_a_open_data.batiment_groupe_synthese_propriete_usage usage
		ON groupe.batiment_groupe_id = usage.batiment_groupe_id
	WHERE groupe.annee_construction BETWEEN 2016 AND 2021
  		AND groupe.contient_fictive_geom_groupe IS NOT TRUE
  		AND (usage.usage_principal_bdnb_open IS NULL OR usage.usage_principal_bdnb_open NOT LIKE 'Dépendance')
	);


---Ajout d'une géométrie ponctuelle et d'index spatiaux
ALTER TABLE nat.nouvelles_constructions_16_21
ADD COLUMN geompoint geometry;

UPDATE nat.nouvelles_constructions_16_21
SET geompoint = ST_PointOnSurface(geom_groupe);

CREATE INDEX nat_nouvelles_constructions_16_21_geom_idx ON nat.nouvelles_constructions_16_21 USING GIST(geom_groupe);
CREATE INDEX nat_nouvelles_constructions_16_21_geompoint_idx ON nat.nouvelles_constructions_16_21 USING GIST(geompoint);


--- Ajout de la localisation communale et départementale selon la géographie au 1er janvier 2022
ALTER TABLE nat.nouvelles_constructions_16_21 DROP COLUMN IF EXISTS idcom22 CASCADE;
ALTER TABLE nat.nouvelles_constructions_16_21 DROP COLUMN IF EXISTS idcom22 CASCADE;
ALTER TABLE nat.nouvelles_constructions_16_21 ADD COLUMN idcom22 varchar(5);
ALTER TABLE nat.nouvelles_constructions_16_21 DROP COLUMN IF EXISTS iddep21 CASCADE;
ALTER TABLE nat.nouvelles_constructions_16_21 DROP COLUMN IF EXISTS iddep22 CASCADE;
ALTER TABLE nat.nouvelles_constructions_16_21 ADD COLUMN iddep22 varchar(3);

UPDATE nat.nouvelles_constructions_16_21 a
SET idcom22 = b.insee_com, iddep22= b.insee_dep
FROM insee.geocom22 b
WHERE ST_Contains(b.geom, a.geompoint);

UPDATE nat.nouvelles_constructions_16_21 a
SET idcom22 =
	CASE
		WHEN idcom22 LIKE '132%' THEN '13055'
		WHEN idcom22 LIKE '75%' THEN '75056'
		WHEN idcom22 LIKE '6938%' THEN '69123'
	END,
	iddep22 = 
	CASE
		WHEN idcom22 LIKE '132%' THEN '13'
		WHEN idcom22 LIKE '75%' THEN '75'
		WHEN idcom22 LIKE '6938%' THEN '69'
	END
WHERE idcom22 LIKE '132%'
	OR idcom22 LIKE '75%'
	OR idcom22 LIKE '6938%';

CREATE INDEX nat_nouvelles_constructions_16_21_idcom22_idx ON nat.nouvelles_constructions_16_21(idcom22);
CREATE INDEX nat_nouvelles_constructions_16_21_iddep22_idx ON nat.nouvelles_constructions_16_21(iddep22);
