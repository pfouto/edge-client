mvn clean package

echo "Copying to cluster"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* cluster:/home/pfouto/edge/client
echo "Copying to grid"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* nancy.g5k:/home/pfouto/edge/client
echo "Done"