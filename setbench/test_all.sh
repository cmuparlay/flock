
for file in bin/*
do
  echo "running $file"
  "$file" -i
  "$file" -n 1000000 -r 3
done