----------
---Ce script attribue un score de mobilisation potentielle des espaces mobilisables dans des opérations d'aménagement
----------

DO
$do$

DECLARE
	millesime text;
	millesime_short text;
	dep varchar(2);
	table_cominterco_nat text;
	table_cominterco text;
	index_scorpat text;
	index_lastgeompar_v text;
	index_lastgeompoint_v text;
	surface_batiment text;
	schema_ff_dep text;
	table_dep_local text;
	
BEGIN


FOREACH millesime IN ARRAY ARRAY['2009','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']

LOOP
        RAISE NOTICE 'Traitement du millésime % en cours',millesime;
        
        millesime_short := RIGHT(millesime,2);
        table_cominterco_nat := 'public'||millesime_short;
        index_scorpat := 'nat_'||table_cominterco_nat||'_scorpat_idx';
	index_lastgeompar_v := 'nat_'||table_cominterco_nat||'_lastgeompar_v_idx';
	index_lastgeompoint_v := 'nat_'||table_cominterco_nat||'_lastgeompoint_v_idx';
	schema_ff_dep := 'ff_'||millesime||'_dep';
		
	IF millesime::int <= 2017 THEN
		surface_batiment := 'spevtot';
	ELSE
		surface_batiment := 'slocal';
	END IF;
	
	
	RAISE NOTICE 'Suppression-création des colonnes et indexes';
			     
	EXECUTE format(
		$$
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS lastgeompar_nobat CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS lastgeompar_v CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS lastgeompoint_v CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS support_bati CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS support_eau CASCADE;
		ALTER TABLE nat.%1$I ADD COLUMN lastgeompar_nobat geometry;
		ALTER TABLE nat.%1$I ADD COLUMN lastgeompar_v geometry;
		ALTER TABLE nat.%1$I ADD COLUMN lastgeompoint_v geometry;
		ALTER TABLE nat.%1$I ADD COLUMN support_bati bool;
		ALTER TABLE nat.%1$I ADD COLUMN support_eau bool;
		
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS typpat CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS scorpat CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS nature CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS area CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS indice_i CASCADE;
		ALTER TABLE nat.%1$I DROP COLUMN IF EXISTS indice_miller CASCADE;
		ALTER TABLE nat.%1$I ADD COLUMN typpat text;
		ALTER TABLE nat.%1$I ADD COLUMN scorpat int;
		ALTER TABLE nat.%1$I ADD COLUMN nature text;
		ALTER TABLE nat.%1$I ADD COLUMN area numeric;
		ALTER TABLE nat.%1$I ADD COLUMN indice_i numeric;
		ALTER TABLE nat.%1$I ADD COLUMN indice_miller numeric;
		
		CREATE INDEX %2$I ON nat.%1$I(scorpat);
		CREATE INDEX %3$I ON nat.%1$I USING gist(lastgeompar_v);
		CREATE INDEX %4$I ON nat.%1$I USING gist(lastgeompoint_v);
		$$,
		table_cominterco_nat,
		index_scorpat,
		index_lastgeompar_v,
		index_lastgeompoint_v
		);
	COMMIT;
        
	FOR dep IN EXECUTE 'SELECT ccodep FROM nat.' || table_cominterco_nat || ' GROUP BY ccodep ORDER BY ccodep'		
	LOOP
	
       		RAISE NOTICE 'Département %',dep;
       		
       		table_cominterco := 'public'||millesime_short||'_'||dep;
       		
       		IF millesime::int <= 2017 THEN
			table_dep_local := 'd'||LOWER(dep)||'_'||millesime||'_pb0010_local';
		ELSE
			table_dep_local := 'd'||LOWER(dep)||'_fftp_'||millesime||'_pb0010_local';
		END IF;
				

		---Parcelles hors terre ferme
		RAISE NOTICE 'Parcelles hors terre ferme';
		EXECUTE format(
			$$
			UPDATE nat.%1$I
			SET scorpat = 0,
				typpat = 'Anomalie géométrique',
				nature = 'Hors terre ferme'
			WHERE cominterco IS TRUE
				AND geomloc IS NOT NULL
				AND idpar NOT IN
					(SELECT idpar
					FROM nat.%1$I a
					JOIN insee.georeg22 b
						ON ST_Intersects(a.geomloc,b.geom));
			$$,
			table_cominterco);
		COMMIT;
		
		
		---Traitement des parcelles bâties mais non vectorisées
		RAISE NOTICE 'Parcelles bâties mais non vectorisées';
		EXECUTE format(
			$$
			WITH sub AS (
				SELECT a.idpar, a.idbat, SUM(a.%1$s/
					CASE
						WHEN a.dnbniv::int <= 0 OR a.dnbniv::int IS NULL THEN 1
						ELSE a.dnbniv::int
					END) surface_emprise_batiment
				FROM %2$I.%3$I a
				JOIN nat.%4$I b ON a.idpar = b.idpar
				WHERE b.cominterco IS TRUE
					AND b.scorpat IS NULL
					AND b.geomok IS FALSE
					AND b.nbat > 0
				GROUP BY a.idpar, a.idbat),

			sub2 AS (
				SELECT idpar, SUM(surface_emprise_batiment) emprises_s
				FROM sub
				GROUP BY idpar)

			UPDATE nat.%4$I a
			SET scorpat = CASE
					WHEN a.dcntpa - b.emprises_s <= 0 THEN 0
				END,
				typpat = CASE
					WHEN a.dcntpa - b.emprises_s <= 0 THEN 'Parcelle complètement bâtie'
				END,
				nature = CASE
					WHEN a.dcntpa - b.emprises_s <= 0 THEN 'Parcelle non vectorisée complètement bâtie'
				END,
				area = CASE
					WHEN a.dcntpa - b.emprises_s <= 0 THEN dcntpa
					ELSE a.dcntpa - b.emprises_s
				END
			FROM sub2 b
			WHERE a.idpar = b.idpar
			$$,
			surface_batiment,
			schema_ff_dep,
			table_dep_local,
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
				FROM nat.%1$I a, nat.%1$I b
				WHERE a.cominterco IS TRUE
					AND b.cominterco IS TRUE
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
			FROM nat.%1$I a, nat.%1$I b
			WHERE a.cominterco IS TRUE
				AND a.scorpat IS NULL
				AND b.cominterco IS TRUE
				AND b.scorpat IS NULL
				AND a.geomok IS FALSE
				AND b.geomok IS TRUE
				AND a.idpk > b.idpk
				AND ST_Within(a.geomloc,b.lastgeompar);
				
			CREATE INDEX temp_cominterco_superpose_todelete_idx ON temp.cominterco_superpose_todelete(ccodep,idpar);

			UPDATE nat.%1$I a
			SET scorpat = 0,
				typpat = 'Anomalie géométrique',
				nature = 'En superposition'
			FROM temp.cominterco_superpose_todelete b
			WHERE a.ccodep = b.ccodep
				AND a.idpar = b.idpar;

			DROP TABLE temp.cominterco_superpose_todelete CASCADE;
			$$,
			table_cominterco);
		COMMIT;


		---Soustraction des espaces bâtis des parcelles vectorisées
		RAISE NOTICE 'Soustraction des espaces bâtis des parcelles vectorisées';
		EXECUTE format(
			$$
			WITH sub AS (
				SELECT a.idpar, ST_MakeValid(ST_Union(ST_Intersection(a.lastgeompar,b.geom_groupe))) geom_poly
				FROM nat.%1$I a
				JOIN bdnb_2024_10_a_open_data.batiment_groupe b ON ST_Intersects(a.lastgeompar,b.geom_groupe)
				WHERE a.cominterco IS TRUE
					AND a.scorpat IS NULL
					AND a.geomok IS TRUE
					AND b.contient_fictive_geom_groupe IS FALSE
					AND (b.annee_construction IS NULL OR b.annee_construction < %2$L::int)
				GROUP BY a.idpar
			)
			
			UPDATE nat.%1$I a
			SET lastgeompar_nobat = ST_MakeValid(ST_CollectionExtract(ST_Difference(a.lastgeompar,b.geom_poly),3)),
				support_bati = TRUE
			FROM sub b
			WHERE a.idpar = b.idpar;
			
			UPDATE nat.%1$I
			SET scorpat = 0,
				typpat = 'Parcelle complètement bâtie',
				nature = 'Parcelle vectorisée complètement bâtie',
				area = ST_Area(lastgeompar)
			WHERE cominterco IS TRUE
				AND support_bati IS TRUE
				AND (ST_Area(lastgeompar_nobat) IS NULL OR ST_Area(lastgeompar_nobat) <= 0);
			
			UPDATE nat.%1$I
			SET lastgeompar_nobat = lastgeompar,
				support_bati = FALSE
			WHERE cominterco IS TRUE
				AND scorpat IS NULL 
				AND lastgeompar_nobat IS NULL;
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
				FROM nat.%1$I a
				JOIN ign_topo.surface_eau_estran_2021 b
					ON a.lastgeompar && b.geom_poly
				WHERE a.cominterco IS TRUE
					AND a.scorpat IS NULL
					AND a.geomok IS TRUE
				GROUP BY a.idpar
			)
			
			UPDATE nat.%1$I a
			SET lastgeompar_v = ST_MakeValid(ST_CollectionExtract(ST_Difference(a.lastgeompar_nobat,b.geom_poly),3)),
				support_eau = TRUE
			FROM sub b
			WHERE a.idpar = b.idpar;	
			
			UPDATE nat.%1$I
			SET scorpat = 1,
				typpat = 'Espace hydrographique',
				nature = 'Parcelle complètement sous eaux',
				area = ST_Area(lastgeompar_nobat)
			WHERE cominterco IS TRUE
				AND support_eau IS TRUE
				AND (ST_Area(lastgeompar_v) IS NULL OR ST_Area(lastgeompar_v) <= 0);
			
			UPDATE nat.%1$I
			SET lastgeompar_v = lastgeompar_nobat,
				support_eau = FALSE
			WHERE cominterco IS TRUE
				AND scorpat IS NULL 
				AND lastgeompar_v IS NULL;
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---dilatation-erosion et simplification
		RAISE NOTICE 'dilatation-erosion et simplification des géométries';
		EXECUTE format(
			$$
			UPDATE nat.%1$I
			SET lastgeompar_v =
				ST_MakeValid(ST_SnapToGrid(ST_Simplify(ST_Buffer(ST_CollectionExtract(ST_Buffer(ST_Buffer(lastgeompar_v,3),-6),3),3),0.01),0.01))
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE;
				
			UPDATE nat.%1$I
			SET scorpat = 1,
				typpat = 'Contrainte morphologique',
				nature = 'Parcelle inférieure à 3 m de large'
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE
				AND (ST_Area(lastgeompar_v) IS NULL OR ST_Area(lastgeompar_v) <= 0);
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---centroide et recalcul des surfaces
		RAISE NOTICE 'centroide et recalcul des surfaces';
		EXECUTE format(
			$$
			UPDATE nat.%1$I
			SET lastgeompoint_v = CASE
					WHEN scorpat IS NULL AND lastgeompar_v IS NULL THEN geomloc
					ELSE ST_PointOnSurface(lastgeompar_v)
					END,
				area = CASE
					WHEN area IS NOT NULL
						THEN area
					WHEN area IS NULL
						AND (support_bati IS TRUE OR support_eau IS TRUE)
						THEN ST_Area(lastgeompar_v)
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
			UPDATE nat.%1$I
			SET indice_i = CASE
					WHEN lastgeompar_v IS NOT NULL THEN area/ST_Perimeter(lastgeompar_v)
					END,
				indice_miller = CASE
					WHEN lastgeompar_v IS NOT NULL THEN (4*pi()*area)/((ST_Perimeter(lastgeompar_v))^2)
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
			UPDATE nat.%1$I
			SET scorpat = 1,
				typpat = CASE
					WHEN area < 200
						OR (indice_miller < 0.3 AND indice_i < 2)
						OR (indice_miller < 0.1 AND indice_i < 5)
						OR schemrem > dcntpa*0.66
						THEN 'Contrainte morphologique'
					ELSE 'Contrainte géographique'
					END,
				nature = CASE
					WHEN schemrem > dcntpa*0.66 THEN 'Chemin de remembrement'
					WHEN pente_pc_mean > 15.71 THEN 'Pente forte'---seules 10%% de parcelles sont bâties sur des pentes supérieures
					WHEN indice_miller < 0.1 AND indice_i < 5 THEN 'Scorie type 2'
					WHEN indice_miller < 0.3 AND indice_i < 2 THEN 'Scorie type 1'
					WHEN area < 200 THEN 'Surface < 200 m2'
				END
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND (area < 200
					OR (indice_miller < 0.3 AND indice_i < 2)
					OR (indice_miller < 0.1 AND indice_i < 5)
					OR schemrem > dcntpa*0.66
					OR pente_pc_mean > 15.71);
			$$,
			table_cominterco
			);
		COMMIT;
		
		---Attribution par distance au bâti
		RAISE NOTICE 'attribution par distance au bâti';
		EXECUTE format(
			$$
			UPDATE nat.%1$I par
			SET scorpat = 9
			WHERE cominterco IS TRUE
				AND scorpat IS NULL
				AND geomok IS TRUE
				AND EXISTS
					(SELECT 1
					FROM bdnb_2024_10_a_open_data.batiment_groupe bat
					WHERE bat.contient_fictive_geom_groupe IS FALSE
						AND ST_Dwithin(COALESCE(par.lastgeompar_v,par.lastgeompoint_v), bat.geom_groupe, 1000));---Si pas de géométrie surface, alors prendre la géométrie ponctuelle
						---pas de critère de date du bâtiment --> tous les bâtiments en 2024, pour prendre en compte les réserves foncières éloignées
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		EXECUTE format(
			$$
			UPDATE nat.%1$I
			SET scorpat = 1,
				typpat = 'Contrainte géographique',
				nature = 'Parcelle éloignée de tout bâti'
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
				  ST_Collect(ST_Intersection(l.geometrie, p.lastgeompar_v)) AS geom_intersect
				FROM 
				  ign_topo.troncon_de_route_2021 l,
				  nat.%1$I p
				WHERE p.cominterco IS TRUE
					AND p.scorpat = 9
					AND ST_Intersects(l.geometrie, p.lastgeompar_v)
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
					ST_Collect(ST_Intersection(r.geom, par.lastgeompar_v)) geom_r
				FROM temp.routes_locales r, nat.%1$I par
				WHERE par.cominterco IS TRUE
					AND ST_Intersects(r.geom, par.lastgeompar_v)
				GROUP BY par.idpar
			);
			CREATE INDEX temp_routes_locales_cut_geom_idx ON temp.routes_locales_cut USING gist(geom_r);
			DROP TABLE IF EXISTS temp.routes_locales CASCADE;

			---Qualification des parcelles municipales composées à + de 33%% de voirie
			UPDATE nat.%1$I par
			SET scorpat = 2,
				typpat = 'Transport',
				nature = 'Voirie'
			FROM temp.routes_locales_cut r
			WHERE ST_Intersects(r.geom_r,par.lastgeompar_v)
				AND ST_Area(ST_Intersection(r.geom_r,par.lastgeompar_v)) > par.area*0.33;
				
			DROP SCHEMA temp CASCADE;
			$$,
			table_cominterco);
			
		COMMIT;
		
		
		---voie ferrée
		RAISE NOTICE 'attribution des voies ferrées';
		EXECUTE format(
			$$
			UPDATE nat.%1$I a
			SET scorpat = 2,
				typpat = 'Transport',
				nature = 'Voie ferrée'
			FROM ign_topo.troncon_de_voie_ferree_2021 b
			WHERE cominterco IS TRUE
				AND a.scorpat = 9
				AND b.nature NOT LIKE 'Métro'
				AND b.position_par_rapport_au_sol LIKE '0'
				AND b.etat_de_l_objet LIKE 'En service'
				AND ST_Intersects(a.lastgeompar_v, b.geometrie);
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---attribution par type de propriétaire ou de groupe de culture
		RAISE NOTICE 'attribution par type de propriétaire ou de groupe de culture';
		EXECUTE format(
			$$
			UPDATE nat.%1$I
			SET scorpat = CASE
					WHEN cgrnumd LIKE '10' THEN 4
					WHEN cgrnumd LIKE '11' THEN 3
					WHEN cgrnumd LIKE '08' THEN 1
					WHEN com_niv BETWEEN 3 AND 4 THEN 4
					WHEN com_niv = 7 THEN 2
					ELSE 2
				END,
				typpat = CASE
					WHEN cgrnumd LIKE '10' THEN 'Terrain à bâtir'
					WHEN cgrnumd LIKE '07' THEN 'Industriel et commercial'
					WHEN cgrnumd LIKE '08' THEN 'Espace hydrographique'
					WHEN cgrnumd LIKE '11' THEN 'Culture et loisirs'
					WHEN com_niv BETWEEN 3 AND 4 THEN 'Propriétaire délégué à la maitrise foncière'
					WHEN com_niv = 7 THEN 'Propriétaire de communaux'
				END,
				nature = CASE
					WHEN cgrnumd LIKE '07' THEN 'Carrière'
					WHEN cgrnumd LIKE '08' THEN 'Espace déclaré comme aquatique ou marécageux'
					WHEN cgrnumd LIKE '11' THEN 'Espace déclaré comme jardin et terrain d agrément'
					WHEN cgrnumd LIKE '10' THEN 'Terrain à bâtir'
				END
			WHERE cominterco IS TRUE
				AND scorpat = 9
				AND (com_niv = 7 OR com_niv BETWEEN 3 AND 4
					OR cgrnumd LIKE '10' OR cgrnumd LIKE '07' OR cgrnumd LIKE '08' OR cgrnumd LIKE '11')
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		
		---attribution d une fonction par données spatiales exogènes
		RAISE NOTICE 'attribution d une fonction par données spatiales exogènes';
		EXECUTE format(
			$$
			UPDATE nat.%1$I a
			SET scorpat = CASE
					WHEN (b.source LIKE 'equipement_de_transport_2021' OR b.source LIKE 'toponymie_transport_2021')
						AND b.nature LIKE 'Parking'
						THEN 3
					WHEN b.source LIKE 'terrain_de_sport_2021' OR b.source LIKE 'zai_cut_2021' THEN 3
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
						OR b.nature LIKE 'Déchèterie'
						OR b.nature LIKE 'Vestige%%'
						OR categorie LIKE 'Gestion des eaux')
						THEN 2
					WHEN b.source LIKE 'zai_cut_2021' AND b.nature LIKE 'Carrière' THEN 2
					WHEN b.source LIKE 'basol' OR b.source LIKE 'cimetiere_2021' THEN 2	
					ELSE 9
				END,
				typpat = b.categorie,
				nature = b.nature
			FROM ign_topo.contraintes_jointes b
			WHERE a.cominterco IS TRUE
				AND a.scorpat = 9
				AND (ST_Within(a.lastgeompoint_v,b.geom_poly)
					OR ST_Within(b.geom_point,a.lastgeompar_v));
			$$,
			table_cominterco
			);
		COMMIT;
		
		
		---attribution par propriétaire orienté service
		RAISE NOTICE 'attribution par propriétaire orienté service';
		EXECUTE format(
			$$
			UPDATE nat.%1$I
			SET scorpat = 3,
				typpat = 'Propriétaire orienté service',
				nature = CASE
						WHEN com_niv = 5 THEN 'Divers'
						ELSE 'HLM'
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
			UPDATE nat.%1$I a
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
					WHEN b.nature LIKE 'Fo%%' THEN 'Administratif ou militaire'
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
			UPDATE nat.%1$I
			SET scorpat = 4,
				typpat = 'Espace non assigné'
			WHERE scorpat = 9				
			$$,
			table_cominterco
			);
		COMMIT;
	
	END LOOP;
END LOOP;

RAISE NOTICE 'Tous les millésimes ont été traités';

END

$do$
