--- Ce script sert à rassembler les schémas départementaux d'une version de DV3F en une seul schéma national et un seul schéma départemental
--- Il est écrit pour la version 7 de DV3F mais peut être adapté pour une autre version en remplaçant "dv3fv7" par la version concernée

--- CREATION DU SCHEMA DEPARTEMENTAL FINAL
CREATE SCHEMA IF NOT EXISTS dv3fv7_dep;

--- INTEGRATION DU NOM DU DEPARTEMENT DANS LE NOM DE CHAQUE TABLE
--- Executer manuellement les requêtes générées par le code ci-dessous
SELECT 'ALTER TABLE '||table_schema||'.'||table_name||' RENAME TO '||RIGHT(table_schema,3)||'_'||table_name||';' 
   FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE' AND table_schema LIKE 'dvf_d%';

--- RENOMMAGE DES CONTRAINTES
--- Executer manuellement les requêtes générées par le code ci-dessous
SELECT 'ALTER TABLE '||constraint_schema||'.'||table_name||' RENAME CONSTRAINT '||constraint_name||' TO '||RIGHT(table_schema,3)||'_'||constraint_name||';' 
   FROM information_schema.table_constraints
   WHERE table_schema LIKE 'dvf_d%' AND CAST(LEFT(constraint_name,1) AS text) NOT IN ('0','1','2','3','4','5','6','7','8','9');

--- CHANGEMENT DE SCHEMA DES TABLES DEPARTEMENTALES
--- Executer manuellement les requêtes générées par le code ci-dessous
SELECT 'ALTER TABLE '||table_schema||'.'||table_name||' SET SCHEMA dv3fv7_dep;' 
   FROM information_schema.tables 
   WHERE table_type = 'BASE TABLE' AND table_schema LIKE 'dvf_d%';

--- SUPPRESSION DES SCHEMAS DEPARTEMENTAUX VIDES
--- Executer manuellement les requêtes générées par le code ci-dessous
SELECT 'DROP SCHEMA '||schema_name||' CASCADE;'
FROM information_schema.schemata
WHERE schema_name LIKE 'dvf_d%';

--- RENOMMAGE DU SCHEMA INITIAL EN SCHEMA NATIONAL
ALTER SCHEMA dvf RENAME TO dv3fv7;
ALTER SCHEMA dvf_annexe RENAME TO dv3fv7_annexe;
