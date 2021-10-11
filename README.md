# Completezza serie inla

Programma per la selezione delle serie da utilizzare per il modello INLA sulle differenze 2019/2020

Questo programma scarica le serie dal database del progetto PULVIRUS e seleziona le serie da utilizzare per il modello INLA sulle differenze giornaliere 2019/2020. I criteri di completezza adottati per il progetto PULVIRUS richiedono che le serie siano "complete" nel corseo del periodo 2016-2020. Inoltre questi criteri richiedono che gli anni oggetti di analisi siano validi in tutte le stagioni.

Il modello sviluppato per il paper utilizza le stazioni selezionate mediante i criteri di completezza originali definiti per il progetto PULVIRUS. Questi criteri vanno bene per il nord Italia dove il numero delle stazioni e' sufficientemente denso. Quando invece si considera l'Italia intera questi criteri diventano troppo restrittivi.

Il modello INLA sulle differenze considera solo i mesi di marzo e aprile 2019/2020. I criteri di completezza di PULVIRUS invece considerano la completezza delle serie su tuute le stagioni sul periodo 2016-2020.

Il programma `selezionaSerieComplete_modelloDifferenze` seleziona dal database le serie degli inquinanti rilassando i criteri di completezza di PULVIRUS, ovvero:

- considerando solo gli anni 2019/2020
- considerando solo la stagione primaverile (ovvero la stagione dei mesi oggetto di analisi marzo e aprile)

Questi criteri di completezza vengono utilizzati per far girare il modello INLA sulle differenze 2019/2020 su tutta ITALIA.
