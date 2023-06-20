mvn clean package

echo "Copying to cluster"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/client/* cluster:/home/pfouto/edge/client
echo "Copying to grid"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/client/* nancy.g5k:/home/pfouto/edge/client
echo "Done"