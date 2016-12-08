package com.camillepradel.movierecommender.model;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import com.camillepradel.movierecommender.controller.MainController;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Rating {
	
    private Movie movie;
    private int userId;
    private int score;

    public Rating() {
        this.movie = null;
        this.userId = 0;
        this.score = 0;
    }

    public Rating(Movie movie, int userId, int score) {
        this.movie = movie;
        this.userId = userId;
        this.score = score;
    }

    public Rating(Movie movie, int userId) {
        this.movie = movie;
        this.userId = userId;
        this.score = 0;
    }

	public Movie getMovie() {
        return this.movie;
    }

    public void setMovie(Movie movie) {
		this.movie = movie;
	}

    // pseudo getter and setter for movie id
    // (in order to automatically serialize Rating object on form submission)

	public int getMovieId() {
        return this.movie.getId();
    }

    public void setMovieId(int movieId) {
    	boolean isMongoDB = MainController.isMongoDB;

        this.movie = isMongoDB ? getMovieFromMongoDB(movieId) : getMovieFromNeo4j(movieId);
    }

    private Movie getMovieFromNeo4j(int movieId) {
    	Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "jul31280" ) );
		Session session = driver.session();

		StatementResult result = session.run("MATCH (m:Movie{id:"+movieId+"})-[t]->(g:Genre) return m.title,m.id,g.name,g.id");
		
		String previousMovieTitle = "";
		int previousMovieId = 0;
		
		List<Genre> genres = new ArrayList<>();
		Movie searchedMovie = null;
		while (result.hasNext() && searchedMovie == null) {
		    Record record = result.next();
		    
		    String movieTitle = record.get("m.title").asString();
		    int id = record.get("m.id").asInt();
		    String genreTitle = record.get("g.name").asString();
		    int genreId = record.get("g.id").asInt();
		    
		    if (id != previousMovieId) {
		    	previousMovieTitle = movieTitle;
		    	previousMovieId = id;
		    }
		    
		    genres.add(new Genre(genreId,genreTitle));
		}

		session.close();
		driver.close();
		
		return new Movie(previousMovieId,previousMovieTitle,genres);
    }
    
    private Movie getMovieFromMongoDB(int movieId) {
    	MongoClient mongo = null;
		try {
			 mongo = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = mongo.getDB("MovieLens");
		DBCollection tableMovie = db.getCollection("movies");
		
    	BasicDBObject query = new BasicDBObject("_id",movieId);
    	
		DBObject movie = tableMovie.findOne(query);
		
		int genreId = 0;
		int id = (int)movie.get("_id");
		String title = (String) movie.get("title");
		BasicDBList genres_raw = (BasicDBList) movie.get("genres");
		System.out.println("SetMovie : "+movieId+" - "+id);
		ArrayList<Genre> genres = new ArrayList<Genre>();
			
		for (int i = 0; i<genres_raw.size(); i++) {
			String titleGenre = (String)genres_raw.get(i);
			genres.add(new Genre(genreId, titleGenre));
			genreId++;
		}
		
		return new Movie(id, title, genres);
    }
    
    public int getUserId() {
        return this.userId;
    }

	public void setUserId(int userId) {
		this.userId = userId;
	}

    public int getScore() {
        return this.score;
    }

	public void setScore(int score) {
		this.score = score;
	}
}