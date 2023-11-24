-- Ajout d'une contrainte UNIQUE sur la colonne num_dep de la table departements
ALTER TABLE departements
ADD CONSTRAINT departements_num_dep_key
UNIQUE (num_dep);

-- Ajout d'une contrainte FOREIGN KEY sur la colonne dep de la table urgences_hosp
-- faisant référence à la colonne num_dep de la table departements
ALTER TABLE urgences_hosp
ADD CONSTRAINT urgences_hosp_dep_fkey
FOREIGN KEY (dep)
REFERENCES departements (num_dep);

-- Ajout d'une contrainte UNIQUE sur les colonnes num_dep et date_passage de la table hospitalisation
ALTER TABLE hospitalisation
ADD CONSTRAINT hospitalisation_key
UNIQUE (num_dep, date_passage);

-- Ajout d'une contrainte FOREIGN KEY sur les colonnes dep et date_de_passage de la table urgences_hosp
-- faisant référence aux colonnes num_dep et date_passage de la table hospitalisation
ALTER TABLE urgences_hosp
ADD CONSTRAINT urgences_hosp_hospitalisation_fkey
FOREIGN KEY (dep, date_de_passage)
REFERENCES hospitalisation (num_dep, date_passage);

-- Ajout d'une contrainte UNIQUE sur les colonnes num_dep, code_tranches_dage et date_de_passage de la table passage_urgence
ALTER TABLE passage_urgence
ADD CONSTRAINT passage_urgence_key
UNIQUE (num_dep, code_tranches_dage, date_de_passage);

-- Ajout d'une contrainte FOREIGN KEY sur les colonnes dep, sursaud_cl_age_corona et date_de_passage de la table urgences_hosp
-- faisant référence aux colonnes num_dep, code_tranches_dage et date_de_passage de la table passage_urgence
ALTER TABLE urgences_hosp
ADD CONSTRAINT urgences_hosp_passage_urgence_fkey
FOREIGN KEY (dep, sursaud_cl_age_corona, date_de_passage)
REFERENCES passage_urgence (num_dep, code_tranches_dage, date_de_passage);

