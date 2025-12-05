## volmetd


the problem i ran into is that our pvc monitoring via node exporter in kubernetes was not able to add proper labels for pvc names in digitalocean

so i made this. its sort of hacky, but it seems to work for us on our doks cluster.

