-- Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au nombre de passages total pour les femmes par an et par département ?
SELECT 
    u.dep,
    EXTRACT(YEAR FROM u.date_de_passage) AS annee,
    (SUM(u.nbre_pass_corona_f) * 100.0 / NULLIF(SUM(u.nbre_pass_tot_f), 0)) AS pourcentage_covid_femmes
FROM 
    urgences_hosp u
GROUP BY 
    u.dep, annee
ORDER BY 
    u.dep, annee;
-- Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au nombre de passages total, pour les personnes âgées de plus de 65 ans, en 2023 ?
SELECT 
    (SUM(u.nbre_pass_corona) * 100.0 / NULLIF(SUM(u.nbre_pass_tot), 0)) AS pourcentage_covid
FROM 
    urgences_hosp u
WHERE 
    EXTRACT(YEAR FROM u.date_de_passage) = 2023
    AND u.sursaud_cl_age_corona = '65-74 ans' -- Mettez la tranche d'âge appropriée
GROUP BY 
    u.sursaud_cl_age_corona;
