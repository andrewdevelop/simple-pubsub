VSN := "2.2.0"

tag:
	git tag -a $(VSN) -m "Release $(VSN)"
	git push origin --tags