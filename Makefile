VSN := "2.2.1"

tag:
	git add .
	git commit -m "upd: $(VSN)"
	git tag -a $(VSN) -m "Release $(VSN)"
	git push origin --tags