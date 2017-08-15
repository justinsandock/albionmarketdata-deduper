echo off
kubectl.exe --namespace albiondata set image deploy/albiondata-deduper albiondata-deduper=us.gcr.io/personal-projects-1369/albiondata/deduper:%1