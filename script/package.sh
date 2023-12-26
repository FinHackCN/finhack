rm -rf dist
rm -rf finhack.egg-info
python setup.py sdist
twine upload dist/*
