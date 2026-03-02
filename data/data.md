=========================================
draft_simple.csv (par part pour pouvoir push sur git)
=========================================

présente 280 000 lignes. 
chaque ligne représente une partie.

match_id : l'id du match créé par riot API
serveur : le serveur sur lequel s'est déroulé la partie (europe west : euw, korea : kr)
patch : la version du jeu
elo : le niveau global de la partie classée
blue_side_win : vrai si l'équipe bleu a gagné, faux si l'équipe rouge a gagné
blue_top_champion : le nom du champion (personnage) occupant le rôle top dans l'équipe bleue
blue_jungle_champion,
blue_mid_champion,
blue_adc_champion,
blue_support_champion,

blue_top_puuid : l'id (riot api id) du joueur humain qui contrôle le personnage au top du côté bleu
blue_jungle_puuid,
blue_mid_puuid,
blue_adc_puuid,
blue_support_puuid,

red_top_champion,
red_jungle_champion,
red_mid_champion,
red_adc_champion,
red_support_champion,
red_top_puuid,
red_jungle_puuid,
red_mid_puuid,
red_adc_puuid,
red_support_puuid,

blue_ban_1 : le champion bani par un joueur jouant du coôté bleu
blue_ban_2,
blue_ban_3,
blue_ban_4,
blue_ban_5,
red_ban_1,
red_ban_2,
red_ban_3,
red_ban_4,
red_ban_5

exemple : 
EUW1_7517331727, euw1, 15.17.708.5788, HIGH_ELO, False, Sett, Talon, Diana,Jinx,Rell,,,,,,Yasuo,FiddleSticks,Cassiopeia,Aphelios,Bard,,,,,,Lulu,Fiora,Rengar,Vex,Poppy,Dr. Mundo,Garen,Milio,Zoe,Draven


=========================================
games_pro_2025_very_light.csv
=========================================
présente des games d'équpies professionnels en tournois officiel.
il y a eu 10 000 parties en 2025. Nous n'utilisons pas ces données.

=========================================
2025_pro_games.csv
=========================================
est la données brute des games pros de 2025 récupérées sur https://oracleselixir.com/tools/downloads.


=========================================
df_Simple_WR_FULL.csv
=========================================
présente des données scrappées sur https://dpm.lol/tierlist
Il y a des données sur les performances d'un champion en fonction du serveur, patch et elo dans lequel il est joué.
pour elo et server, "TOUT" désigne "en moyenne sur tous les elos/serveurs"
le patch est toujours la version du jeu
label est le classement du champion par rapport aux autres champions (en terme de tier)
le role est le rôle sur lequel on analyse le champion (l'influence du côté de la map n'est pas analysé blue/red side)
role_pickrate est la fréquence à laquelle ce champion est joué à ce rôle
tier est une note globale de succès du champion
le winrate est ce qui nous intéresse le plus : nombre de victoires / nombre de parties qu'il a jouées
winrate_evol est l'évolution de son winrate par rapport au dernier patch.
le pickrate est le nombre de parties dans lesquelles le champion est joué / nombre de parties qui ont été jouées.
games est le nombre de parties dans lesquelles le champion a été joué.


elo,server,patch,label,name,role,role_pickrate,tier,winrate,winrate_evol,pickrate,games
TOUT,TOUT,16.3,1,Briar,jun,91.1%,S+,52.8%,+1.5%,6.7%,599 576


=========================================
WR_matchups_TOUTTOUT15.24
=========================================
dans ce dossier, on trouve des données de performances de matchups et de synergies (à quel point un champion est fort contre/ avec un autre). (TOUTTOUT15 indique que cela concerne le patch 15.24, et que les données sont obtenues en moyenne sur tous les elos et tous les serveurs)

dans ce dossier, il y a un dossier par matchup/synergie existante.
Par exemple, dans le dossier "top synergy", on va trouver une liste de couple de champions. celui de gauche avec son propre rôle et celui de droite joué au top et dans la même équipe que celui de gauche.

champion est le nom du champion sur lequel les mesures sont faites
role est le rôle qu'il joue
role_play_ratio est la proportion des parties du champion jouées dans ce rôle
tier est le niveau de performance global du champion dans ce rôle (ex : S, A, B, C).
Indicateur synthétique basé sur les métriques statistiques.
rank est la position du champion dans le classement de son rôle (plus le nombre est petit, meilleur est le classement).
winrate est le taux de victoire du champion dans ce rôle
pickrate est le taux de sélection du champion dans ce rôle (proportion des parties où il est choisi).
banrate est le taux de bannissement du champion
nb_games_analyzed est le nombre total de parties utilisées pour calculer les statistiques.
url est le lien vers la source des données ou la page détaillée du champion.

synergy_top_1_name
Nom du champion top avec lequel la synergie est mesurée.

synergy_top_1_winrate
Taux de victoire du duo.

synergy_top_1_games
Nombre de parties analysées pour ce duo.

synergy_top_1_lane_quality
Indicateur de performance en phase de lane (ce label n'est rempli que pour les matchups sur le même rôle).

matchup_mid_2_name :
nom du champion mid contre lequel les données sont mesurées,

matchup_mid_2_winrate :
winrate contre le champion matchup_mid_2_name,

matchup_mid_2_games :
nombre de games sur lesquelles sont faites les mesures,

matchup_mid_2_lane_quality :
dans le cas où le champion analisé n'est pas mid, cette valeur est null. sinon, elle représente qualitativement si champion a l'avantage et à quel point.


champion,role,role_play_ratio,tier,rank,winrate,pickrate,banrate,nb_games_analyzed,url,collect_coherente,synergy_top_1_name,synergy_top_1_winrate,synergy_top_1_games,synergy_top_1_lane_quality,synergy_top_2_name,synergy_top_2_winrate,synergy_top_2_games,synergy_top_2_lane_quality,
Miss Fortune,adc,97.3%,S+,1 / 26,52.2 %,22.6 %,18.5 %,5439399,https://dpm.lol/champions/MissFortune/build?lane=bottom&tier=all&platform=all&timeframe=15.24,False,Warwick,54.91%,37958,,Urgot,54.86%,88843,,Singed,54.79%,69144,,

=========================================
champions_15.1.1_15.24.1.csv
=========================================
présente des données relatives aux champions (personnages) telles que leurs dégâts d'attaques, leurs points de défence etc. Nous utilisons ces données pour remplacer les noms des champions par des données numériques. C'est une facon d'encoder.

patch,id,key,name,title,partype,tags,lore,blurb,allytips,enemytips,skins,skins_count,info_attack,info_defense,info_magic,info_difficulty,stats_hp,stats_hpperlevel,stats_mp,stats_mpperlevel,stats_movespeed,stats_armor,stats_armorperlevel,stats_spellblock,stats_spellblockperlevel,stats_attackrange,stats_hpregen,stats_hpregenperlevel,stats_mpregen,stats_mpregenperlevel,stats_crit,stats_critperlevel,stats_attackdamage,stats_attackdamageperlevel,stats_attackspeedperlevel,stats_attackspeed,passive_name,passive_description,passive_image,spell_q_id,spell_q_name,spell_q_description,spell_q_tooltip,spell_q_leveltip,spell_q_maxrank,spell_q_cooldown,spell_q_cooldownBurn,spell_q_cost,spell_q_costBurn,spell_q_datavalues,spell_q_effect,spell_q_effectBurn,spell_q_vars,spell_q_costType,spell_q_maxammo,spell_q_range,spell_q_rangeBurn,spell_q_image,spell_q_resource,spell_w_id,spell_w_name,spell_w_description,spell_w_tooltip,spell_w_leveltip,spell_w_maxrank,spell_w_cooldown,spell_w_cooldownBurn,spell_w_cost,spell_w_costBurn,spell_w_datavalues,spell_w_effect,spell_w_effectBurn,spell_w_vars,spell_w_costType,spell_w_maxammo,spell_w_range,spell_w_rangeBurn,spell_w_image,spell_w_resource,spell_e_id,spell_e_name,spell_e_description,spell_e_tooltip,spell_e_leveltip,spell_e_maxrank,spell_e_cooldown,spell_e_cooldownBurn,spell_e_cost,spell_e_costBurn,spell_e_datavalues,spell_e_effect,spell_e_effectBurn,spell_e_vars,spell_e_costType,spell_e_maxammo,spell_e_range,spell_e_rangeBurn,spell_e_image,spell_e_resource,spell_r_id,spell_r_name,spell_r_description,spell_r_tooltip,spell_r_leveltip,spell_r_maxrank,spell_r_cooldown,spell_r_cooldownBurn,spell_r_cost,spell_r_costBurn,spell_r_datavalues,spell_r_effect,spell_r_effectBurn,spell_r_vars,spell_r_costType,spell_r_maxammo,spell_r_range,spell_r_rangeBurn,spell_r_image,spell_r_resource,winrate

15.1.1,
Aatrox,
266,
Aatrox,
Épée des Darkin,
Puits de sang,
"[""Fighter""]",
"Autrefois, Aatrox et ses frères étaient honorés pour avoir défendu Shurima contre le Néant. Mais ils finirent par devenir une menace plus grande encore pour Runeterra : la ruse et la sorcellerie furent employées pour les battre. Cependant, après des siècles d'emprisonnement, Aatrox fut le premier à retrouver sa liberté, en corrompant et transformant les mortels assez stupides pour tenter de s'emparer de l'arme magique qui contenait son essence. Désormais en possession d'un corps qu'il a approximativement transformé pour rappeler son ancienne forme, il arpente Runeterra en cherchant à assouvir sa vengeance apocalyptique.","Autrefois, Aatrox et ses frères étaient honorés pour avoir défendu Shurima contre le Néant. Mais ils finirent par devenir une menace plus grande encore pour Runeterra : la ruse et la sorcellerie furent employées pour les battre. Cependant, après des...",

"[""Utilisez Ruée obscure tout en lançant Épée des Darkin pour augmenter vos chances de toucher l'ennemi."", ""Facilitez Épée des Darkin avec des compétences de contrôle de foule, telles que Chaînes infernales, ou avec les effets immobilisants de vos alliés."", ""Lancez Fossoyeur des mondes quand vous êtes certain de pouvoir forcer le combat.""]",

"[""Les attaques d'Aatrox sont prévisibles. Profitez-en pour esquiver ses zones d'impact."", ""Il est plus facile de fuir les Chaînes infernales d'Aatrox en courant vers un côté ou vers Aatrox."", ""Quand Aatrox utilise son ultime, gardez vos distances pour l'empêcher de revenir à la vie.""]",

"[{""id"": ""266000"", ""num"": 0, ""name"": ""default"", ""chromas"": false}, {""id"": ""266001"", ""num"": 1, ""name"": ""Aatrox justicier"", ""chromas"": false}, {""id"": ""266002"", ""num"": 2, ""name"": ""Mecha Aatrox"", ""chromas"": true}, {""id"": ""266003"", ""num"": 3, ""name"": ""Aatrox chasseur marin"", ""chromas"": false}, {""id"": ""266007"", ""num"": 7, ""name"": ""Aatrox lune de sang"", ""chromas"": false}, {""id"": ""266008"", ""num"": 8, ""name"": ""Aatrox lune de sang prestige"", ""chromas"": false}, {""id"": ""266009"", ""num"": 9, ""name"": ""Aatrox héros de guerre"", ""chromas"": true}, {""id"": ""266011"", ""num"": 11, ""name"": ""Aatrox de l'Odyssée"", ""chromas"": true}, {""id"": ""266020"", ""num"": 20, ""name"": ""Aatrox lune de sang prestige (2022)"", ""chromas"": false}, {""id"": ""266021"", ""num"": 21, ""name"": ""Aatrox de l'éclipse lunaire"", ""chromas"": true}, {""id"": ""266030"", ""num"": 30, ""name"": ""DRX Aatrox"", ""chromas"": true}, {""id"": ""266031"", ""num"": 31, ""name"": ""DRX Aatrox prestige"", ""chromas"": false}, {""id"": ""266033"", ""num"": 33, ""name"": ""Aatrox primordien"", ""chromas"": true}]",

13,8,4,3,4,650,114,0,0.0,345,38,4.8,32,2.05,175,3.0,0.5,0.0,0.0,0,0,60,5.0,2.5,0.651,

Posture du massacreur,"Régulièrement, la prochaine attaque de base d'Aatrox inflige des <physicalDamage>dégâts physiques</physicalDamage> supplémentaires et le soigne, selon un pourcentage des PV max de la cible. ","{""full"": ""Aatrox_Passive.png"", ""sprite"": ""passive0.png"", ""group"": ""passive"", ""x"": 0, ""y"": 0, ""w"": 48, ""h"": 48}",AatroxQ,Épée des Darkin,"Aatrox abat son épée devant lui, infligeant des dégâts physiques. Il peut frapper jusqu'à 3 fois et chaque coup a une zone d'effet différente.","Aatrox abat son épée, infligeant <physicalDamage>{{ qdamage }} pts de dégâts physiques</physicalDamage>. Si l'ennemi est touché par le tranchant, il est brièvement <status>projeté dans les airs</status> et subit <physicalDamage>{{ qedgedamage }} pts de dégâts</physicalDamage> à la place. Cette compétence peut être <recast>réactivée</recast> deux fois, chaque coup changeant de forme et infligeant 25% de dégâts de plus que la précédente.{{ spellmodifierdescriptionappend }}","{""label"": [""Délai de récupération"", ""Dégâts"", ""Ratio de dégâts d'attaque totaux""], ""effect"": [""{{ cooldown }} -> {{ cooldownNL }}"", ""{{ qbasedamage }} -> {{ qbasedamageNL }}"", ""{{ qtotaladratio*100.000000 }}% -> {{ qtotaladrationl*100.000000 }}%""]}",5,"[14, 12, 10, 8, 6]",14/12/10/8/6,"[0, 0, 0, 0, 0]",0,{},"[null, [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]","[null, ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0""]",[],Pas de coût,-1,"[25000, 25000, 25000, 25000, 25000]",25000,"{""full"": ""AatroxQ.png"", ""sprite"": ""spell0.png"", ""group"": ""spell"", ""x"": 384, ""y"": 48, ""w"": 48, ""h"": 48}",Pas de coût,AatroxW,Chaînes infernales,"Aatrox frappe le sol, blessant le premier ennemi touché. Les champions et les grands monstres doivent vite quitter la zone d'effet sous peine d'être ramenés de force au point d'impact et de subir à nouveau les dégâts.","Aatrox lance une chaîne, <status>ralentissant</status> le premier ennemi touché de {{ wslowpercentage*-100 }}% pendant {{ wslowduration }} sec et infligeant <magicDamage>{{ wdamage }} pts de dégâts magiques</magicDamage>. Les champions et les grands monstres de la jungle doivent quitter la zone d'effet dans les {{ wslowduration }} sec sous peine d'être <status>ramenés de force</status> au point d'impact et de subir à nouveau les dégâts.{{ spellmodifierdescriptionappend }}","{""label"": [""Délai de récupération"", ""Dégâts"", ""Ralentissement""], ""effect"": [""{{ cooldown }} -> {{ cooldownNL }}"", ""{{ wbasedamage }} -> {{ wbasedamageNL }}"", ""{{ wslowpercentage*-100.000000 }}% -> {{ wslowpercentagenl*-100.000000 }}%""]}",5,"[20, 18, 16, 14, 12]",20/18/16/14/12,"[0, 0, 0, 0, 0]",0,{},"[null, [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]","[null, ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0""]",[],Pas de coût,-1,"[825, 825, 825, 825, 825]",825,"{""full"": ""AatroxW.png"", ""sprite"": ""spell0.png"", ""group"": ""spell"", ""x"": 432, ""y"": 48, ""w"": 48, ""h"": 48}",Pas de coût,AatroxE,Ruée obscure,"Passivement, Aatrox se soigne quand il blesse des champions ennemis. À l'activation, il se rue dans une direction.",<spellPassive>Passive :</spellPassive> Aatrox récupère des PV équivalents à <lifeSteal>{{ totalevamp }}</lifeSteal> des dégâts qu'il inflige aux champions.<br /><br /><spellActive>Active :</spellActive> Aatrox se rue dans une direction. Il peut utiliser cette compétence tout en lançant ses autres compétences.{{ spellmodifierdescriptionappend }},"{""label"": [""Délai de récupération""], ""effect"": [""{{ cooldown }} -> {{ cooldownNL }}""]}",5,"[9, 8, 7, 6, 5]",9/8/7/6/5,"[0, 0, 0, 0, 0]",0,{},"[null, [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]","[null, ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0""]",[],Pas de coût,-1,"[25000, 25000, 25000, 25000, 25000]",25000,"{""full"": ""AatroxE.png"", ""sprite"": ""spell0.png"", ""group"": ""spell"", ""x"": 0, ""y"": 96, ""w"": 48, ""h"": 48}",Pas de coût,AatroxR,Fossoyeur des mondes,"Aatrox libère sa forme démoniaque, effrayant les sbires ennemis proches et augmentant ses dégâts d'attaque, ses soins et sa vitesse de déplacement. La durée est prolongée s'il participe à l'élimination d'un champion ennemi.","Aatrox révèle sa vraie forme démoniaque, <status>effrayant</status> les sbires proches pendant {{ rminionfearduration }} sec et gagnant <speed>+{{ rmovementspeedbonus*100 }}% de vitesse de déplacement</speed> (ce bonus diminue en {{ rduration }} sec). Il gagne aussi <scaleAD>+{{ rtotaladamp*100 }}% de dégâts d'attaque</scaleAD> et augmente ses <healing>soins personnels de {{ rhealingamp*100 }}%</healing> pendant la durée.<br /><br />Participer à l'élimination d'un champion prolonge la durée de cet effet de {{ rextension }} sec et réinitialise le bonus en <speed>vitesse de déplacement</speed>.{{ spellmodifierdescriptionappend }}","{""label"": [""Total du bonus en dégâts d'attaque"", ""Augmentation des soins"", ""Vitesse de déplacement"", ""Délai de récupération""], ""effect"": [""{{ rtotaladamp*100.000000 }}% -> {{ rtotaladampnl*100.000000 }}%"", ""{{ rhealingamp*100.000000 }}% -> {{ rhealingampnl*100.000000 }}%"", ""{{ rmovementspeedbonus*100.000000 }}% -> {{ rmovementspeedbonusnl*100.000000 }}%"", ""{{ cooldown }} -> {{ cooldownNL }}""]}",3,"[120, 100, 80]",120/100/80,"[0, 0, 0]",0,{},"[null, [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0]]","[null, ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0"", ""0""]",[],Pas de coût,-1,"[25000, 25000, 25000]",25000,"{""full"": ""AatroxR.png"", ""sprite"": ""spell0.png"", ""group"": ""spell"", ""x"": 48, ""y"": 96, ""w"": 48, ""h"": 48}",Pas de coût,0.501607