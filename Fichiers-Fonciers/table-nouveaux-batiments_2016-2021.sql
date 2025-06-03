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
