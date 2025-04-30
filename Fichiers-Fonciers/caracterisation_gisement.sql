DO
$do$

DECLARE
	millesime text;
	millesime_short text;
	dep varchar(2);
	table_cominterco_nat text;
	table_cominterco text;
	index_scorpat text;
	index_lastgeompar_nobat_nettoye text;
	
BEGIN


FOREACH millesime IN ARRAY ARRAY['2009','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']

LOOP
        RAISE NOTICE 'Traitement du millésime % en cours',millesime;
        
        millesime_short := RIGHT(millesime,2);
        table_cominterco_nat := 'public'||millesime_short;
        index_scorpat := 'nat_'||table_cominterco_nat||'_scorpat_idx';
	index_lastgeompar_nobat_nettoye := 'nat_'||table_cominterco_nat||'_lastgeompar_nobat_nettoye_idx';
	
	RAISE NOTICE 'Suppression-création des colonnes et indexes';
			     
	EXECUTE format(
		$$
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS lastgeompar_nobat CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS lastgeompar_nobat_nettoye CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS lastgeompoint_nobat_nettoye CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS support_bati CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS support_eau CASCADE;
		ALTER TABLE nat.%1$s ADD COLUMN lastgeompar_nobat geometry;
		ALTER TABLE nat.%1$s ADD COLUMN lastgeompar_nobat_nettoye geometry;
		ALTER TABLE nat.%1$s ADD COLUMN lastgeompoint_nobat_nettoye geometry;
		ALTER TABLE nat.%1$s ADD COLUMN support_bati bool;
		ALTER TABLE nat.%1$s ADD COLUMN support_eau bool;
		
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS typpat CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS scorpat CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS nature CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS area CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS indice_i CASCADE;
		ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS indice_miller CASCADE;
		ALTER TABLE nat.%1$s ADD COLUMN typpat text;
		ALTER TABLE nat.%1$s ADD COLUMN scorpat int;
		ALTER TABLE nat.%1$s ADD COLUMN nature text;
		ALTER TABLE nat.%1$s ADD COLUMN area numeric;
		ALTER TABLE nat.%1$s ADD COLUMN indice_i numeric;
		ALTER TABLE nat.%1$s ADD COLUMN indice_miller numeric;
		
		CREATE INDEX %2$s ON nat.%1$s(scorpat);
		CREATE INDEX %3$s ON nat.%1$s USING gist(lastgeompar_nobat_nettoye);
		$$,
		table_cominterco_nat,
		index_scorpat,
		index_lastgeompar_nobat_nettoye
		);
	COMMIT;
        
	FOR dep IN EXECUTE 'SELECT ccodep FROM nat.' || table_cominterco_nat || ' GROUP BY ccodep ORDER BY ccodep'		
	LOOP
	
       		RAISE NOTICE 'Département %',dep;
       		
       		table_cominterco := 'public'||millesime_short||'_'||dep;
			
			
		---Suppression des parcelles sans géométrie valide déclarées comme bâties
		RAISE NOTICE 'Parcelles sans géométrie mais bâties';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = 0,
				typpat = 'sans géométrie - bâti'
			WHERE cominterco IS TRUE
				AND geomok IS FALSE
				AND nbat > 0
			$$,
			table_cominterco);
		COMMIT;


		---Parcelles hors terre ferme
		RAISE NOTICE 'Parcelles hors terre ferme';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = 0,
				typpat = 'hors terre ferme'
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE
				AND idpar NOT IN
					(SELECT idpar
					FROM nat.%1$s a
					JOIN insee.georeg22 b
						ON ST_Intersects(a.geomloc,b.geom));
			$$,
			table_cominterco);
		COMMIT;

		---Plus petites parcelles en superposition
		RAISE NOTICE 'Plus petites parcelles en superposition';
		EXECUTE format(
			$$
			CREATE SCHEMA IF NOT EXISTS temp;
			
			DROP TABLE IF EXISTS temp.cominterco_superpose_todelete CASCADE;
			CREATE TABLE temp.cominterco_superpose_todelete AS (
				SELECT DISTINCT b.ccodep,b.idpar
				FROM nat.%1$s a, nat.%1$s b
				WHERE a.cominterco IS TRUE
					AND a.scorpat IS NULL
					AND b.cominterco IS TRUE
					AND b.scorpat IS NULL
					AND a.geomok IS TRUE---uniquement pour les parcelles vectorisées
					AND b.geomok IS TRUE
					AND a.idpk > b.idpk
					AND a.lastgeompar && b.lastgeompar
					AND ST_Area(ST_Intersection(a.lastgeompar,b.lastgeompar))
						> ST_Area(b.lastgeompar)*0.33
					GROUP BY b.ccodep,b.idpar
					);
			
			---Insert des parcelles avec géométrie ponctuelle uniquement, intersectant un polygone
			INSERT INTO temp.cominterco_superpose_todelete
			SELECT DISTINCT a.ccodep,a.idpar
			FROM nat.%1$s a, nat.%1$s b
			WHERE a.cominterco IS TRUE
				AND a.scorpat IS NULL
				AND b.cominterco IS TRUE
				AND b.scorpat IS NULL
				AND a.geomok IS FALSE
				AND b.geomok IS TRUE
				AND a.idpk > b.idpk
				AND ST_Within(a.geomloc,b.lastgeompar);
				
			CREATE INDEX temp_cominterco_superpose_todelete_idx ON temp.cominterco_superpose_todelete(ccodep,idpar);

			UPDATE nat.%1$s a
			SET scorpat = 0,
				typpat = 'en superposition'
			FROM temp.cominterco_superpose_todelete b
			WHERE a.ccodep = b.ccodep
				AND a.idpar = b.idpar;

			DROP TABLE temp.cominterco_superpose_todelete CASCADE;
			$$,
			table_cominterco);
		COMMIT;


		---Soustraction des espaces bâtis
		RAISE NOTICE 'Soustraction des espaces bâtis';
		EXECUTE format(
			$$
			WITH sub AS (
				SELECT a.idpar, ST_MakeValid(ST_Union(ST_Intersection(a.lastgeompar,b.geom_groupe))) geom_poly
				FROM nat.%1$s a
				JOIN bdnb_2024_10_a_open_data.batiment_groupe b	ON ST_Intersects(a.lastgeompar,b.geom_groupe)
				LEFT JOIN bdnb_2024_10_a_open_data.batiment_groupe_ffo_bat c ON b.batiment_groupe_id = c.batiment_groupe_id
				WHERE a.cominterco IS TRUE
					AND a.scorpat IS NULL
					AND a.geomok IS TRUE
					AND b.contient_fictive_geom_groupe IS FALSE
					AND (c.annee_construction IS NULL OR c.annee_construction < %2$L::int)
				GROUP BY a.idpar
			)
			
			UPDATE nat.%1$s a
			SET lastgeompar_nobat = ST_MakeValid(ST_CollectionExtract(ST_Difference(a.lastgeompar,b.geom_poly),3)),
				support_bati = TRUE
			FROM sub b
			WHERE a.idpar = b.idpar;
			
			UPDATE nat.%1$s
			SET scorpat = 0,
				typpat = 'complètement bati'
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE
				AND (ST_Area(lastgeompar_nobat) IS NULL OR ST_Area(lastgeompar_nobat) <= 0);
			
			UPDATE nat.%1$s
			SET lastgeompar_nobat = lastgeompar,
				support_bati = FALSE
			WHERE cominterco IS TRUE
				AND scorpat IS NULL 
				AND lastgeompar_nobat_nettoye IS NULL;
			$$,
			table_cominterco,
			millesime
			);
		COMMIT;
					
		---Soustraction des zones hydrographiques
		RAISE NOTICE 'Soustraction des zones hydrographiques';
		EXECUTE format(
			$$
			WITH sub AS (
				SELECT a.idpar,
					ST_MakeValid(ST_Union(
						(ST_Intersection(a.lastgeompar_nobat,b.geom_poly)))) geom_poly
				FROM nat.%1$s a
				JOIN ign_topo.surface_eau_estran_2021 b
					ON a.lastgeompar && b.geom_poly
				WHERE a.cominterco IS TRUE
					AND a.scorpat IS NULL
					AND a.geomok IS TRUE
				GROUP BY a.idpar
			)
			
			UPDATE nat.%1$s a
			SET lastgeompar_nobat_nettoye = ST_MakeValid(ST_CollectionExtract(ST_Difference(a.lastgeompar_nobat,b.geom_poly),3)),
				support_eau = TRUE
			FROM sub b
			WHERE a.idpar = b.idpar;	
			
			UPDATE nat.%1$s
			SET scorpat = 0,
				typpat = 'complètement sous eaux'
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE
				AND (ST_Area(lastgeompar_nobat_nettoye) IS NULL OR ST_Area(lastgeompar_nobat_nettoye) <= 0);
			
			UPDATE nat.%1$s
			SET lastgeompar_nobat_nettoye = lastgeompar_nobat,
				support_eau = FALSE
			WHERE cominterco IS TRUE
				AND scorpat IS NULL 
				AND lastgeompar_nobat_nettoye IS NULL;
			$$,
			table_cominterco
			);
		COMMIT;
		
		---dilatation-erosion et simplification
		RAISE NOTICE 'dilatation-erosion et simplification des géométries';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET lastgeompar_nobat_nettoye =
				ST_MakeValid(ST_SnapToGrid(ST_Simplify(ST_Buffer(ST_CollectionExtract(ST_Buffer(ST_Buffer(lastgeompar_nobat_nettoye,5),-10),3),5),0.01),0.01))
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE;
				
			UPDATE nat.%1$s
			SET scorpat = 0,
				typpat = 'Disparu après dilatation-érosion'
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE
				AND (ST_Area(lastgeompar_nobat_nettoye) IS NULL OR ST_Area(lastgeompar_nobat_nettoye) <= 0);
			$$,
			table_cominterco
			);
		COMMIT;
		
		---centroide et recalcul des surfaces
		RAISE NOTICE 'centroide et recalcul des surfaces';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET lastgeompoint_nobat_nettoye = CASE
					WHEN scorpat IS NULL AND lastgeompar_nobat_nettoye IS NULL THEN geomloc
					ELSE ST_PointOnSurface(lastgeompar_nobat_nettoye)
					END,
				area = CASE
					WHEN support_bati IS TRUE OR support_eau IS TRUE THEN ST_Area(lastgeompar_nobat_nettoye)
					ELSE dcntpa
					END
			WHERE cominterco IS TRUE
			$$,
			table_cominterco
			);
		COMMIT;
			
		---Analyse morphologique
		RAISE NOTICE 'analyse morphologique';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET indice_i = CASE
					WHEN lastgeompar_nobat_nettoye IS NOT NULL THEN area/ST_Perimeter(lastgeompar_nobat_nettoye)
					END,
				indice_miller = CASE
					WHEN lastgeompar_nobat_nettoye IS NOT NULL THEN (4*pi()*area)/((ST_Perimeter(lastgeompar_nobat_nettoye))^2)
					END
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE;
			$$,
			table_cominterco
			);
		COMMIT;
		
		---attribution sur indicateur géométrique, topographique ou surfacique
		RAISE NOTICE 'attribution sur indicateur géométrique, topographique ou surfacique';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = 1,
				typpat = CASE
					WHEN area < 200 THEN 'surface < 200 m2'
					WHEN indice_miller < 0.3 AND indice_i < 2 THEN 'résidu type 1'
					WHEN indice_miller < 0.1 AND indice_i < 5 THEN 'résidu type 2'
					WHEN schemrem > 0 THEN 'chemin de remembrement'
					WHEN pente_pc_mean > 15.71 THEN 'pente forte'---seules 10%% de parcelles sont bâties sur des pentes supérieures
				END
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND (area < 200
					OR (indice_miller < 0.3 AND indice_i < 2)
					OR (indice_miller < 0.1 AND indice_i < 5)
					OR schemrem > 0
					OR pente_pc_mean > 15.71);
			$$,
			table_cominterco
			);
		COMMIT;
		
		---Attribution par distance au bâti
		RAISE NOTICE 'attribution par distance au bâti';
		EXECUTE format(
			$$
			UPDATE nat.%1$s par
			SET scorpat = 9
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND EXISTS
					(SELECT 1
					FROM nat.%1$s par2, bdnb_2024_10_a_open_data.batiment_groupe bat--, ff_general.dep_limitrophes depl
					WHERE par.ccodep = par2.ccodep 
					 	AND par.idpk = par2.idpk -- parcelle par parcelle
					 	--AND par2.ccodep = depl.iddep
					 	--AND UPPER(bat.code_departement_insee) LIKE ANY(depl.limitrophes) -- limite la recherche aux départements limitrophes
						AND bat.contient_fictive_geom_groupe IS FALSE
						AND ST_Dwithin(par.lastgeompar_nobat_nettoye, bat.geom_groupe, 1000));	
						---pas de critère de date du bâtiment --> tous les bâtiments en 2021, pour prendre en compte les réserves foncières éloignées
			$$,
			table_cominterco
			);
		COMMIT;
		
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = 1,
				typpat = 'Isolé'
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---attribution de la voirie
		RAISE NOTICE 'attribution de la voirie';
		EXECUTE format(
			$$
			DROP SCHEMA IF EXISTS temp CASCADE;
			CREATE SCHEMA temp;

			---Extraction des voiries pertinentes dans le département
			DROP TABLE IF EXISTS temp.troncon_route_local CASCADE;
			CREATE TABLE temp.troncon_route_local AS(
				SELECT
				  l.cleabs,
				  l.nature,
				  l.nombre_de_voies,
				  l.largeur_de_chaussee,
				  ST_Collect(ST_Intersection(l.geometrie, p.lastgeompar_nobat_nettoye)) AS geom_intersect
				FROM 
				  ign_topo.troncon_de_route_2021 l,
				  nat.%1$s p
				WHERE p.cominterco IS TRUE
					AND p.scorpat = 9
					AND ST_Intersects(l.geometrie, p.lastgeompar_nobat_nettoye)
					AND l.nature NOT LIKE 'Bac%%'
					AND l.fictif IS FALSE
				GROUP BY l.cleabs,
				  l.nature,
				  l.nombre_de_voies,
				  l.largeur_de_chaussee 
				);

			--- Création des largeurs de voirie
			ALTER TABLE temp.troncon_route_local ADD COLUMN geom_buffer geometry;

			UPDATE temp.troncon_route_local
			SET geom_buffer = ST_MakeValid(ST_Buffer(geom_intersect,largeur_de_chaussee));

			UPDATE temp.troncon_route_local l
			SET geom_buffer = ST_MakeValid(ST_Buffer(l.geom_intersect,lrs.largeur_total))
			FROM ign_topo.largeur_routes_simplifiee lrs
			WHERE (l.largeur_de_chaussee IS NULL OR l.largeur_de_chaussee <= 0)
				AND l.nature = lrs.nature
				AND COALESCE(l.nombre_de_voies,0) = lrs.nb_voies;
				
			CREATE INDEX temp_troncon_route_local_geom_idx ON temp.troncon_route_local USING gist(geom_buffer);

			---Fusion des tronçons qui s'intersectent
			CREATE TABLE temp.routes_locales AS (
				SELECT 
				  ST_Union(geom_buffer) AS geom 
				FROM (
				  SELECT 
				   	geom_buffer, 
				    ST_ClusterDBSCAN(geom_buffer, 0, 1) OVER() AS cluster_id 
				  FROM temp.troncon_route_local
				) AS clusters 
				GROUP BY cluster_id
				);
			CREATE INDEX temp_routes_locales_geom_idx ON temp.routes_locales USING gist(geom);
			DROP TABLE IF EXISTS temp.troncon_route_local CASCADE;

			---Découpage des tronçons de voirie par parcelle
			CREATE TABLE temp.routes_locales_cut AS (
				SELECT par.idpar,
					ST_Collect(ST_Intersection(r.geom, par.lastgeompar_nobat_nettoye)) geom_r
				FROM temp.routes_locales r, nat.%1$s par
				WHERE par.cominterco IS TRUE
					AND ST_Intersects(r.geom, par.lastgeompar_nobat_nettoye)
				GROUP BY par.idpar
			);
			CREATE INDEX temp_routes_locales_cut_geom_idx ON temp.routes_locales_cut USING gist(geom_r);
			DROP TABLE IF EXISTS temp.routes_locales CASCADE;

			---Qualification des parcelles municipales composées à + de 33%% de voirie
			UPDATE nat.%1$s par
			SET scorpat = 2, typpat = 'Voirie cadastrée'
			FROM temp.routes_locales_cut r
			WHERE ST_Intersects(r.geom_r,par.lastgeompar_nobat_nettoye)
				AND ST_Area(ST_Intersection(r.geom_r,par.lastgeompar_nobat_nettoye)) > par.area*0.33
			$$,
			table_cominterco);
			
		COMMIT;
		
		
		---voie ferrée
		RAISE NOTICE 'attribution des voies ferrées';
		EXECUTE format(
			$$
			UPDATE nat.%1$s a
			SET scorpat = 2,
				typpat = 'Voie ferrée'
			FROM ign_topo.troncon_de_voie_ferree_2021 b
			WHERE cominterco IS TRUE
				AND a.scorpat = 9
				AND b.nature NOT LIKE 'Métro'
				AND b.position_par_rapport_au_sol LIKE '0'
				AND b.etat_de_l_objet LIKE 'En service'
				AND ST_Intersects(a.lastgeompar_nobat_nettoye, b.geometrie);
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---attribution par type de propriétaire ou de groupe de culture
		RAISE NOTICE 'attribution par type de propriétaire ou de groupe de culture';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = CASE
					WHEN com_niv BETWEEN 3 AND 4 THEN 4
					WHEN cgrnumd LIKE '10' THEN 4
					WHEN cgrnumd LIKE '11' THEN 3
					ELSE 2
				END,
				typpat = CASE
					WHEN com_niv = 7 THEN 'propriétaire de communaux'
					WHEN com_niv BETWEEN 3 AND 4 THEN 'propriétaire délégué à la maitrise foncière'
					WHEN cgrnumd LIKE '10' THEN 'terrain à bâtir'
					WHEN cgrnumd LIKE '07' THEN 'carrière'
					WHEN cgrnumd LIKE '08' THEN 'espaces déclarés marécageux'
					WHEN cgrnumd LIKE '11' THEN 'Culture et loisirs'
				END,
				nature = CASE
					WHEN cgrnumd LIKE '11' THEN 'espace déclaré comme jardin et terrain d agrement'
					END
			WHERE cominterco IS TRUE
				AND scorpat = 9
				AND (com_niv = 7 OR com_niv BETWEEN 3 AND 4
					OR cgrnumd LIKE '10' OR cgrnumd LIKE '07' OR cgrnumd LIKE '08' OR cgrnumd LIKE '11')
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		
		---attribution d'une fonction par contrainte géométrique
		RAISE NOTICE 'attribution d une fonction par contrainte géométrique';
		EXECUTE format(
			$$
			UPDATE nat.%1$s a
			SET scorpat = CASE
					WHEN b.source LIKE 'basol' OR b.source LIKE 'cimetiere_2021' THEN 2
					WHEN b.source LIKE 'zai_cut_2021' AND b.nature LIKE 'Carrière' THEN 2
					WHEN (b.source LIKE 'equipement_de_transport_2021' OR b.source LIKE 'toponymie_transport_2021')
						AND b.nature NOT LIKE 'Parking'
						THEN 2
					WHEN b.source LIKE 'aerodrome_cut_2021'
						OR b.source LIKE 'piste_d_aerodrome_cut_2021'
						OR b.source LIKE 'transport_par_cable_2021'
						THEN 2
					WHEN b.source LIKE 'zai_cut_2021' AND
						(b.nature LIKE 'Centrale électrique'
						OR b.nature LIKE 'Ouvrage militaire'
						OR b.nature LIKE 'Tombeau'
						OR b.nature LIKE 'Déchèterie'
						OR b.nature LIKE 'Vestige%%'
						OR categorie LIKE 'Gestion des eaux')
						THEN 2
					WHEN (b.source LIKE 'equipement_de_transport_2021' OR b.source LIKE 'toponymie_transport_2021')
						AND b.nature LIKE 'Parking'
						THEN 3
					WHEN b.source LIKE 'terrain_de_sport_2021' OR b.source LIKE 'zai_cut_2021' THEN 3		
					ELSE 9
				END,
				typpat = b.categorie,
				nature = b.nature
			FROM ign_topo.contraintes_jointes b
			WHERE a.cominterco IS TRUE
				AND a.scorpat = 9
				AND (ST_Within(a.lastgeompoint_nobat_nettoye,b.geom_poly)
					OR ST_Within(b.geom_point,a.lastgeompar_nobat_nettoye));
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---attribution par propriétaire orienté service
		RAISE NOTICE 'attribution par propriétaire orienté service';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = 3,
				typpat = CASE
						WHEN com_niv = 5 THEN 'Propriétaire orienté service (divers)'
						ELSE 'Propriétaire orienté service (HLM)'
					END
			WHERE cominterco IS TRUE
				AND scorpat = 9
				AND com_niv BETWEEN 5 AND 6
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---attribution par bâti remarquable dans la parcelle initiale
		RAISE NOTICE 'attribution par bâti remarquable dans la parcelle initiale';
		EXECUTE format(
			$$
			UPDATE nat.%1$s a
			SET scorpat = 3,
				typpat =
				CASE
					WHEN b.nature LIKE 'Ar%%'
						OR b.nature LIKE 'Chât%%'
						OR b.nature LIKE 'Mo%%' 
						OR b.nature LIKE 'To%%' THEN 'Culture et loisirs'
					WHEN b.nature LIKE 'Chap%%'
						OR b.nature LIKE 'Eg%%' THEN 'Religieux'
					WHEN b.nature LIKE 'Tribune' THEN 'Sport'
					WHEN b.nature LIKE 'Fo%%' THEN 'Administratif et militaire'
				END,
				nature = b.nature
			FROM ign_topo.bati_remarquable_2021 b
			WHERE cominterco IS TRUE
				AND a.scorpat = 9
				AND a.iddep22 = b.iddep22
				AND a.idcom22 = b.idcom22
				AND ST_Within(b.geom_point,a.lastgeompar);
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---passage en type 4 du reste
		RAISE NOTICE 'passage en type 4 du reste';
		
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET scorpat = 4
			WHERE scorpat = 9				
			$$,
			table_cominterco
			);
		COMMIT;
	
	END LOOP;
END LOOP;
END

$do$
