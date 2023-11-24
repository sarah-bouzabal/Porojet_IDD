# PROJET “Entrepôt de Données”

# Introduction

L'engagement de Santé publique France dans l'amélioration et la préservation de la santé des populations s'est révélé particulièrement crucial pendant la crise sanitaire induite par la pandémie de COVID-19. En cette période exceptionnelle, l'organisme a assumé la lourde responsabilité de surveiller l'évolution de l'épidémie, d'anticiper divers scénarios et de mettre en œuvre des actions visant à prévenir et à restreindre la propagation du virus à l'échelle nationale.
Face à cette réalité sanitaire complexe, une question essentielle se pose naturellement : comment peut-on suivre de manière efficace l'évolution de l'épidémie de COVID-19 pour obtenir toutes les informations cruciales sur la situation sanitaire?
Pour répondre à cette interrogation, une approche novatrice est explorée à travers la conception d'un entrepôt de données. Un entrepôt de données, en tant que base centralisée, intègre des informations provenant de divers ensembles de données. Ces données deviennent des leviers puissants pour l'analyse, la génération d'informations et la prise de décision.
Afin de rendre l'utilisation des données plus efficiente, l'accent est mis sur la modélisation en étoile de l'entrepôt de données. Cette approche, organisant les données dans un schéma en étoile, offre une structure permettant de simplifier la complexité des informations stockées dans un Data Warehouse.
La modélisation en étoile d'un entrepôt de données se caractérise notamment par une table de fait. Dans cette démarche, des fichiers tels que "code-tranches-dage-donnees-urgences.csv," "donnees-urgences-SOS-medecins.csv," et "departement-region.json" représentent des sources de données clés pour alimenter cette table de fait, offrant ainsi un panorama complet pour éclairer la prise de décision dans le contexte de la gestion de la crise sanitaire.


# Transformation 

Les données actuelles présentent une complexité qui rend leur exploitation peu aisée. Afin de faciliter leur utilisation, il est nécessaire d'appliquer un ensemble spécifique de transformations. Nous débuterons par la visualisation de nos données brutes ensuit par le nettoyage.

# Nettoyage des Données
Pour centraliser les données, nous avons initié le processus en transformant la colonne 'date_de_passage' en type datetime à l'aide de la fonction pd.to_datetime(). Cette opération a permis de gérer les erreurs éventuelles grâce au paramètre 'errors' réglé sur 'coerce'. Ensuite, pour garantir la qualité des données, nous avons supprimé les lignes contenant des dates invalides à l'aide de dropna().
Par ailleurs, nous avons converti la colonne "dep" en type string avec astype(str) pour assurer une manipulation cohérente des données.
En ce qui concerne les colonnes à supprimer, nous avons identifié et éliminé les colonnes indésirables ('nbre_acte_corona', 'nbre_acte_tot', 'nbre_acte_corona_h', 'nbre_acte_corona_f', 'nbre_acte_tot_h', 'nbre_acte_tot_f') de la dataframe avec la méthode drop().
   

Enfin, pour le dataframe df_departements, nous avons converti la colonne "num_dep" en type string pour assurer une cohérence dans la manipulation des données. Le résultat final de cette étape de prétraitement a été affiché avec print(df_departements["num_dep"])



# Modélisation de l'Entrepôt de Données
La modélisation en étoile est une méthode de conception de base de données qui se caractérise par l'utilisation d'une table centrale, appelée table de faits (fact table), entourée de plusieurs tables dimensionnelles. L'idée principale est de centraliser les informations clés dans la table de faits, tandis que les tables dimensionnelles contiennent des informations descriptives qui permettent d'analyser ou de filtrer les données de la table de faits.


![Capture d'écran 2023-11-24 180359](https://github.com/sarah-bouzabal/Porojet_IDD/assets/96074783/b8c1a674-49a3-4abd-a5df-cc656a774641)  

# Tests  

![Capture d'écran 2023-11-24 192358](https://github.com/sarah-bouzabal/Porojet_IDD/assets/96074783/b47498b2-ed6a-465c-86bd-f1191811a57a)
![Capture d'écran 2023-11-24 192504](https://github.com/sarah-bouzabal/Porojet_IDD/assets/96074783/a8bd024b-bed1-4bd8-a8d7-d4dc0f537d8d)

 
# Conclusion
L'exécution:

![Capture d'écran 2023-11-24 192218](https://github.com/sarah-bouzabal/Porojet_IDD/assets/96074783/a8ece89f-f73c-4eb4-8099-c32801001833)






