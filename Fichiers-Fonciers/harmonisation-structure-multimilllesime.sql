--- Ce script sert à donner aux millésimes des Fichiers Fonciers antérieurs à 2018 la même structure que les millésimes suivants :  1 schéma "national" + 1 schéma "départemental" par millésime
--- NB : cet exemple fonctionne pour le millésime 2014. Remplacer toutes les mentions de '14' par le millésime souhaité

--- CREATION DES SCHEMAS FINAUX
CREATE SCHEMA IF NOT EXISTS ff_2014;
CREATE SCHEMA IF NOT EXISTS ff_2014_dep;
CREATE SCHEMA IF NOT EXISTS ffta_2014;
CREATE SCHEMA IF NOT EXISTS ffta_2014_dep;

--- CHANGEMENT DE SCHEMA DES TABLES DEPARTEMENTALES
--- Exécuter manuellement les requêtes SQL générées par le code suivant
SELECT 'ALTER TABLE '||table_schema||'.'||table_name||' SET SCHEMA ff_2014_dep;' 
   FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE' AND table_schema LIKE 'ff_d%14'
UNION
SELECT 'ALTER TABLE '||table_schema||'.'||table_name||' SET SCHEMA ffta_2014_dep;' 
   FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE' AND table_schema LIKE 'ffta_d%14';

--- SUPPRESSION DES SCHEMAS VIDES ET INUTILISES
--- Exécuter manuellement les requêtes SQL générées par le code suivant
SELECT 'DROP SCHEMA '||schema_name||' CASCADE;'
FROM information_schema.schemata
WHERE schema_name ILIKE 'ff_d%14'
UNION
SELECT 'DROP SCHEMA '||schema_name||' CASCADE;'
FROM information_schema.schemata
WHERE schema_name ILIKE 'ffta_d%14';

--- REPARATION DES GEOMETRIES PONCTUELLES DE LA TABLE DES PARCELLES DU DEPARTEMENT 95 MILLESIME 2016
--- /!\ Cette étape n'est nécessaire que pour le millésime 2016 et le département 95
ALTER TABLE ff_2016.d95_2016_pnb10_parcelle
ADD COLUMN geomloc2 geometry(MultiPoint);
UPDATE ff_2016.d95_2016_pnb10_parcelle
SET geomloc2 = ST_Multi(geomloc);
ALTER TABLE ff_2016.d95_2016_pnb10_parcelle
DROP COLUMN geomloc;
ALTER TABLE ff_2016.d95_2016_pnb10_parcelle
RENAME COLUMN geomloc2 TO geomloc;
ALTER TABLE ff_2016.d95_2016_pnb10_parcelle
ALTER COLUMN geomloc TYPE geometry(MultiPoint,2154);

--- STRUCTURATION D'UNE TABLE NATIONALE
--- Exécuter manuellement les requêtes SQL générées par le code suivant
SELECT 'CREATE TABLE ff_2014.fftp'||RIGHT(table_name,LENGTH(table_name)-3)||' (LIKE '||table_schema||'.'||table_name||' INCLUDING DEFAULTS);'
FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE' AND table_schema LIKE 'ff_%14_dep' AND table_name LIKE 'd01%'
UNION
SELECT 'CREATE TABLE ffta_2014.ffta'||RIGHT(table_name,LENGTH(table_name)-3)||' (LIKE '||table_schema||'.'||table_name||' INCLUDING DEFAULTS);'
FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE' AND table_schema LIKE 'ffta_%14_dep' AND table_name LIKE 'd01%';

--- IMPLEMENTATION DE L'HERITAGE
--- Exécuter manuellement les requêtes SQL générées par le code suivant
SELECT 'ALTER TABLE ff_2014_dep.'||table_name||' INHERIT ff_2014.'||'fftp'||RIGHT(table_name,LENGTH(table_name)-3)||';' 
   FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE'
   	AND table_schema LIKE 'ff_2014_dep'
UNION
SELECT 'ALTER TABLE ffta_2014_dep.'||table_name||' INHERIT ffta_2014.'||'ffta'||RIGHT(table_name,LENGTH(table_name)-3)||';' 
   FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE'
   	AND table_schema LIKE 'ffta_2014_dep';
