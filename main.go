package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"github.com/apiarian/sgf"
	"github.com/apiarian/sgf/parse"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	usr, _ := user.Current()

	var (
		dbPath  = flag.String("db-path", filepath.Join(usr.HomeDir, "go-games.db"), "The path to the sqlite3 database to store the data")
		clearDB = flag.Bool("clear-db", false, "Clear an existing db and start over")
		sgfDir  = flag.String("sgf-dir", "", "The directory of SGF files to search recursively")
	)

	flag.Parse()

	if *sgfDir == "" {
		log.Fatal("The -sgf-dir argument must be specified")
	}
	sgfDirStats, err := os.Stat(*sgfDir)
	if os.IsNotExist(err) {
		log.Fatal("Could not find " + *sgfDir)
	}
	if !sgfDirStats.IsDir() {
		log.Fatal(*sgfDir + " does not appear to be a directory")
	}
	log.Println("Going to look for SGF files in", *sgfDir)

	log.Println("Looking for the database at", *dbPath)
	alreadyExists, err := exists(*dbPath)
	if err != nil {
		log.Fatal(err)
	}

	if *clearDB && alreadyExists {
		log.Println("Deleting the old database")
		err := os.Remove(*dbPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	db, err := sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *clearDB || !alreadyExists {
		log.Println("Creating a new database")

		dbInitializationString := `
		create table players (
			id integer primary key not null,
			name text not null,
			network text
		);
		create unique index player_name_network ON players(name, network);
		insert into players (id, name, network) values (0, 'UNKNOWN PLAYER', 'UNKNOWN NETWORK');
		create table games (
			id integer primary key not null,
			black_id integer not null,
			white_id integer not null,
			winner_id integer,
			timestamp text,
			foreign key(black_id) references players(id),
			foreign key(white_id) references players(id)
		);
		`
		_, err = db.Exec(dbInitializationString)
		if err != nil {
			log.Printf("%q: %s\n", err, dbInitializationString)
			return
		}
	}
	getPlayerIdSmt, err := db.Prepare("select id from players where name = ? and network = ?")
	if err != nil {
		log.Printf("error making getPlayerIdSmt: %s\n", err)
		return
	}
	insertPlayerSmt, err := db.Prepare("insert into players (name, network) values (?, ?)")
	if err != nil {
		log.Fatalf("error making insertPlayerSmt: %s\n", err)
	}
	insertGameSmt, err := db.Prepare("insert into games (black_id, white_id, winner_id, timestamp) values (?, ?, ?, ?)")
	if err != nil {
		log.Fatalf("error making insertGameSmt: %s\n", err)
	}

	log.Println("ready to go!")

	done := make(chan struct{})
	defer close(done)

	paths, errc := walkFiles(done, *sgfDir)

	c := make(chan result)
	var wg sync.WaitGroup
	const numProcessors = 20
	wg.Add(numProcessors)
	for i := 0; i < numProcessors; i++ {
		go func() {
			processor(done, paths, c)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c)
	}()

	var playerIdCache = make(map[string]int)

	for r := range c {
		if r.err != nil {
			log.Println("got an error with", r.path, r.err)
			continue
		}
		for _, p := range []string{r.black, r.white} {
			if _, ok := playerIdCache[p]; ok {
				continue
			}
			rows, err := getPlayerIdSmt.Query(p, r.network)
			defer rows.Close()
			if err != nil {
				log.Fatalf("error reading id from database for %s, %s: %s\n", p, r.network, err)
			}
			var idFound bool
			for rows.Next() {
				var id int
				err := rows.Scan(&id)
				if err != nil {
					log.Fatalf("error getting id from database for %s, %s: %s\n", p, r.network, err)
				}
				playerIdCache[p] = id
				idFound = true
			}
			if !idFound {
				result, err := insertPlayerSmt.Exec(p, r.network)
				if err != nil {
					log.Fatalf("error inserting player into database for %s, %s: %s\n", p, r.network, err)
				}
				id, err := result.LastInsertId()
				if err != nil {
					log.Fatalf("error extracting the last insert id for %s, %s: %s\n", p, r.network, err)
				}
				playerIdCache[p] = int(id)
			}
		}
		black_id := playerIdCache[r.black]
		white_id := playerIdCache[r.white]
		var insertError error
		switch r.winnerColor {
		case "B":
			_, insertError = insertGameSmt.Exec(
				black_id,
				white_id,
				black_id,
				r.date.Format(time.RFC3339),
			)
		case "W":
			_, insertError = insertGameSmt.Exec(
				black_id,
				white_id,
				white_id,
				r.date.Format(time.RFC3339),
			)
		default:
			_, insertError = insertGameSmt.Exec(
				black_id,
				white_id,
				nil,
				r.date.Format(time.RFC3339),
			)
		}
		if insertError != nil {
			log.Fatalf("error inserting game: %s\n", err)
		}
	}
	if err := <-errc; err != nil {
		log.Fatal(err)
	}
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(paths)
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			select {
			case paths <- path:
			case <-done:
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return paths, errc
}

type result struct {
	path        string
	black       string
	white       string
	network     string
	winnerColor string
	date        sgf.FuzzyDate
	err         error
}

func processor(done <-chan struct{}, paths <-chan string, c chan<- result) {
	for path := range paths {
		rs := process(path)
		for _, r := range rs {
			select {
			case c <- r:
			case <-done:
				return
			}
		}
	}
}

func process(path string) []result {
	r := []result{
		result{path: path},
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		r[0].err = fmt.Errorf("problem reading file: %s", err)
		return r
	}

	collection, _, err := parse.Parse(data)
	if err != nil {
		r[0].err = fmt.Errorf("problem parsing file: %s", err)
		return r
	}
	for i := range collection {
		// extend the return structure to have the same base data for each GameTree
		// in the collection
		if i > 0 {
			r = append(r, r[0])
		}
	}

	for i, gt := range collection {
		r[i].date, err = gt.StartDate()
		if err != nil {
			r[i].err = fmt.Errorf("error getting date for GameTree: %s", err)
			continue
		}
		r[i].black, err = gt.BlackPlayerName()
		if err != nil {
			r[i].err = fmt.Errorf("error getting black player name for GameTree: %s", err)
			continue
		}
		r[i].white, err = gt.WhitePlayerName()
		if err != nil {
			r[i].err = fmt.Errorf("error getting white player name for GameTree: %s", err)
			continue
		}
		r[i].winnerColor, err = gt.WinnerColor()
		if r[i].winnerColor == "" {
			r[i].err = fmt.Errorf("error getting the winner color for GameTree: %s", err)
			continue
		}
		r[i].network = "sample"
	}
	return r
}
