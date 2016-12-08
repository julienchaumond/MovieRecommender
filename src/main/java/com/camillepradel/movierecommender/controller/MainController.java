package com.camillepradel.movierecommender.controller;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.neo4j.driver.v1.*;

@Controller
public class MainController {
	public static boolean isMongoDB = true;
	
	String message = "Welcome to Spring MVC!";
 
	@RequestMapping("/hello")
	public ModelAndView showMessage(
			@RequestParam(value = "name", required = false, defaultValue = "World") String name) {
		System.out.println("in controller");
 
		ModelAndView mv = new ModelAndView("helloworld");
		mv.addObject("message", message);
		mv.addObject("name", name);
		return mv;
	}

	@RequestMapping(value = "/movieratings", method = RequestMethod.GET)
 	public ModelAndView showMoviesRattings(
 			@RequestParam(value = "user_id", required = true) Integer userId) {
 		System.out.println("GET /movieratings for user " + userId);
 
 		// TODO: write query to retrieve all movies from DB
 		List<Movie> allMovies = isMongoDB ? mongoDBMovieList(null) : neo4jMovieList(null);
 
 		// TODO: write query to retrieve all ratings from the specified user
 		List<Rating> ratings = isMongoDB ? mongoDBRatings(userId) : neo4jRatings(userId);
 
 		ModelAndView mv = new ModelAndView("movieratings");
 		mv.addObject("userId", userId);
 		mv.addObject("allMovies", allMovies);
 		mv.addObject("ratings", ratings);
 
 		return mv;
 	}
 
 	@RequestMapping(value = "/movieratings", method = RequestMethod.POST)
 	public String saveOrUpdateRating(@ModelAttribute("rating") Rating rating) {
 		System.out.println("POST /movieratings for user " + rating.getUserId()
 											+ ", movie " + rating.getMovie().getTitle()
 											+ ", movieid " + rating.getMovie().getId()
											+ ", score " + rating.getScore());
 		
 		if (isMongoDB) {
 			mongoDBUpdateRating(rating);
 		} else {
 			neo4JUpdateRating(rating);
 		}
 		
 		return "redirect:/movieratings?user_id=" + rating.getUserId();
 	}
 	
 	@RequestMapping(value = "/recommendations", method = RequestMethod.GET)
	public ModelAndView ProcessRecommendations(
			@RequestParam(value = "user_id", required = true) Integer userId,
			@RequestParam(value = "processing_mode", required = false, defaultValue = "0") Integer processingMode){
		System.out.println("GET /movieratings for user " + userId);

		// TODO: process recommendations for specified user exploiting other users ratings
		//       use different methods depending on processingMode parameter
		
		List<Rating> recommendations = isMongoDB ? mongoDBRecommandation(userId) : neo4jRecommandation(userId);
		ModelAndView mv = new ModelAndView("recommendations");
		mv.addObject("recommendations", recommendations);

		return mv;
	}
	 
	@RequestMapping("/movies")
	public ModelAndView showMovies(
			@RequestParam(value = "user_id", required = false) Integer userId) {
		System.out.println("show Movies of user " + userId);
		
		List<Movie> movies = isMongoDB ? mongoDBMovieList(userId) : neo4jMovieList(userId);
		
		ModelAndView mv = new ModelAndView("movies");
		mv.addObject("userId", userId);
		mv.addObject("movies", movies);
		return mv;
	}
	
	// ----------- MONGODB ----------- //
	
	private List<Rating> mongoDBRatings(Integer userId) {
		MongoClient mongo = null;
		try {
			 mongo = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = mongo.getDB("MovieLens");
		DBCollection tableMovie = db.getCollection("movies");
		DBCollection tableUser = db.getCollection("utilisateurs");
		
		BasicDBObject queryUser = new BasicDBObject();
		queryUser.put("_id", userId);
		
		BasicDBObject moviesID = new BasicDBObject();
		moviesID.put("movies.movieid", "1");
		moviesID.put("movies.rating", "1");
		moviesID.put("_id", "0");
		DBObject user = tableUser.findOne(queryUser, moviesID);
		HashMap<Integer,Integer> userMovies = new HashMap<>();
		BasicDBList moviesList = (BasicDBList)user.get("movies");
		for (int i = 0; i< moviesList.size(); i++) {
			BasicDBObject movie = (BasicDBObject)moviesList.get(i);
			
			int movieId = (int)movie.get("movieid");
			int note = (int)movie.get("rating");
			
			userMovies.put(movieId,note);
		}
		
		DBCursor cursor;
		
		if (userMovies.size() > 0) {
			BasicDBObject inQuery = new BasicDBObject("$in", userMovies.keySet());
			BasicDBObject query = new BasicDBObject("_id", inQuery);
			
			cursor = tableMovie.find(query);
		} else {
			cursor = tableMovie.find();
		}
		
		ArrayList<Rating> ratings = new ArrayList<>();
		int genreId = 0;
		while(cursor.hasNext()) {
			DBObject movie_raw = cursor.next();
			int id = (int)movie_raw.get("_id");
			String title = (String) movie_raw.get("title");
			
			BasicDBList genres_raw = (BasicDBList) movie_raw.get("genres");
			
			ArrayList<Genre> genres = new ArrayList<Genre>();
			
			for (int i = 0; i<genres_raw.size(); i++) {
				String titleGenre = (String)genres_raw.get(i);
				genres.add(new Genre(genreId, titleGenre));
				genreId++;
			}
			
			int note = userMovies.get(id);
			
			ratings.add(new Rating(new Movie(id, title, genres), userId, note));
		}
		
		cursor.close();
		
		return ratings;
	}
	
	private void mongoDBUpdateRating(Rating rating) {
 		MongoClient mongo = null;
		try {
			 mongo = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = mongo.getDB("MovieLens");
		
		DBCollection tableUser = db.getCollection("utilisateurs");
		
		BasicDBObject queryRatingExists = new BasicDBObject();
		queryRatingExists.put("_id", rating.getUserId());
		
		BasicDBObject movieDictionary = new BasicDBObject();
		movieDictionary.put("movieid", rating.getMovieId()+"");
		
		BasicDBObject elemMatchDictionary = new BasicDBObject();
		elemMatchDictionary.put("$elemMatch", movieDictionary);
		
		queryRatingExists.put("movies", elemMatchDictionary);
		
		DBObject user = tableUser.findOne(queryRatingExists);
		
		BasicDBObject queryUser = new BasicDBObject();
		queryUser.put("_id", rating.getUserId());
		
		if (user != null) {
			BasicDBObject moviesDictionary = new BasicDBObject();
			BasicDBObject ratingDictionary = new BasicDBObject();
			ratingDictionary.put("movieid", rating.getMovieId());
			moviesDictionary.put("movies", ratingDictionary);
			
			BasicDBObject pullUser = new BasicDBObject();
			pullUser.put("$pull", moviesDictionary);
			
			tableUser.update(queryUser,pullUser);
		}
		
		BasicDBObject addToSetDictionary = new BasicDBObject();
		BasicDBObject ratingDictionary = new BasicDBObject();
		
		ratingDictionary.put("movieid", rating.getMovieId());
		ratingDictionary.put("rating", rating.getScore());
		int timestamp = (int) ((new Date()).getTime()/1000);
		ratingDictionary.put("timestamp", timestamp);
		
		addToSetDictionary.put("movies", ratingDictionary);
		
		BasicDBObject updateQuery = new BasicDBObject();
		updateQuery.put("$addToSet", addToSetDictionary);
		
		System.out.println(updateQuery);
		tableUser.update(queryUser,updateQuery);
 	}
 	
	private List<Movie> mongoDBMovieList(Integer userId) {
		MongoClient mongo = null;
		try {
			 mongo = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = mongo.getDB("MovieLens");
		DBCollection tableMovie = db.getCollection("movies");
		
		ArrayList<Integer> userMovies = new ArrayList<>();
		if (userId != null) {
			DBCollection tableUser = db.getCollection("utilisateurs");
		
			BasicDBObject queryUser = new BasicDBObject();
		
			queryUser.put("_id", userId);
		
			BasicDBObject moviesID = new BasicDBObject();
			moviesID.put("movies.movieid", "1");
			moviesID.put("_id", "0");
			DBObject user = tableUser.findOne(queryUser, moviesID);		
			
			BasicDBList moviesList = (BasicDBList)user.get("movies");
			for (int i = 0; i< moviesList.size(); i++) {
				BasicDBObject movie = (BasicDBObject)moviesList.get(i);
				int movieId = (int)movie.get("movieid");
				userMovies.add(movieId); 
			}
		}
		DBCursor cursor;
		
		if (userMovies.size() > 0) {
			BasicDBObject inQuery = new BasicDBObject("$in", userMovies);
			BasicDBObject query = new BasicDBObject("_id", inQuery);
			
			cursor = tableMovie.find(query);
		} else {
			cursor = tableMovie.find();
		}
		
		List<Movie> movies = new LinkedList<Movie>();
		int genreId = 0;
		while(cursor.hasNext()) {
			DBObject movie_raw = cursor.next();
			int id = (int)movie_raw.get("_id");
			String title = (String) movie_raw.get("title");
			BasicDBList genres_raw = (BasicDBList) movie_raw.get("genres");
			
			ArrayList<Genre> genres = new ArrayList<Genre>();
			
			for (int i = 0; i<genres_raw.size(); i++) {
				String titleGenre = (String)genres_raw.get(i);
				genres.add(new Genre(genreId, titleGenre));
				genreId++;
			}
			
			movies.add(new Movie(id, title, genres));
		}
		cursor.close();
		
		return movies;
	}
	
	private List<Rating> mongoDBRecommandation(int userId) {
		MongoClient mongo = null;
		try {
			 mongo = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = mongo.getDB("MovieLens");
		DBCollection tableUser = db.getCollection("utilisateurs");
		
		List<Movie> userMovieList = mongoDBMovieList(userId);
		
		// Unwind
		DBObject unwindQuery = new BasicDBObject("$unwind","$movies");
		// Project
		DBObject projectParametersQuery = new BasicDBObject("_id", "$movies.movieid");
		projectParametersQuery.put("userId","$_id");
		DBObject projectQuery = new BasicDBObject("$project", projectParametersQuery);
		
		// Match
		BasicDBList userMovieIdList = new BasicDBList();
		userMovieList.forEach( m -> userMovieIdList.add(new BasicDBObject("_id",m.getId())));
		
		DBObject orQuery = new BasicDBObject("$or", userMovieIdList);
		orQuery.put("userId", new BasicDBObject("$ne",userId));
		DBObject matchQuery = new BasicDBObject("$match",orQuery);
		// Group
		DBObject groupParameterQuery = new BasicDBObject("_id","$userId");
		groupParameterQuery.put("nbIteration", new BasicDBObject("$sum", 1));
		DBObject groupQuery = new BasicDBObject("$group", groupParameterQuery);
		// Sort
		DBObject sortQuery = new BasicDBObject("$sort", new BasicDBObject("nbIteration", -1));
		// Limit
		DBObject limitQuery = new BasicDBObject("$limit",1);
		
		Iterator<DBObject> result = tableUser.aggregate(unwindQuery,projectQuery,matchQuery, groupQuery, sortQuery, limitQuery).results().iterator();

		List<Rating> recommendations = new ArrayList<>();
		DBObject row = result.next();
		int matchedUserId = (int) row.get("_id");
		List<Rating> otherUserListRating = mongoDBRatings(matchedUserId);
		
		for(Rating r : otherUserListRating) {
			boolean found = false;
			for(Movie m : userMovieList) {
				if (r.getMovieId() == m.getId()) {
					found = true;
				}
			}
			
			if(!found) {
				recommendations.add(r);
			}
		}
		
		return recommendations;
	}
	
	// ------------ NEO4J ------------ //
	
	private List<Rating> neo4jRecommandation(int userId) {
		Date start = new Date();
		System.out.println("Start : "+start.toString());
		String recommandationQuery = "MATCH (target_user:User {id : "+userId+"})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) WITH other_user, count(distinct m.title) AS num_common_movies, target_user"
				+ " ORDER BY num_common_movies DESC"
				+ " LIMIT 1"
				+ " MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)"
				+ " WHERE NOT (target_user)-[:RATED]->(m2)"
				+ " RETURN m2.title AS rec_movie_title, m2.id AS rec_movie_id, rat_other_user.note AS rating,"
				+ " other_user.id AS watched_by ORDER BY rat_other_user.note DESC";
		
		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "jul31280" ) );
		Session session = driver.session();
		
		System.out.println("Before iteration : "+ ((new Date()).getTime() - start.getTime())/1000);
		StatementResult recommandationResult = session.run(recommandationQuery);
		System.out.println("After session run : "+ ((new Date()).getTime() - start.getTime())/1000);
		List<Genre> listGenreEmpty = new ArrayList<>();
		List<Rating> recommendations = new LinkedList<Rating>();
		
		while(recommandationResult.hasNext()) {
			Record record = recommandationResult.next();
			
			String movieTitle = record.get("rec_movie_title").asString();
			int idMovie = record.get("rec_movie_id").asInt();
			int note = record.get("rating").asInt();
			int userIdRecommander = record.get("watched_by").asInt();
			
			recommendations.add(new Rating(new Movie(idMovie,movieTitle,listGenreEmpty),userIdRecommander,note));
		}
		System.out.println("End : "+ ((new Date()).getTime() - start.getTime())/1000);
		return recommendations;
	}
	
	private void neo4JUpdateRating(Rating rating) {
		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "jul31280" ) );
		Session session = driver.session();
	
		StatementResult alreadyRated = session.run("MATCH (u:User{id:"+rating.getUserId()+"})-[r:RATED]->(m:Movie{id:"+rating.getMovieId()+"}) return r");
		
		if (!alreadyRated.hasNext()) {
			session.run("MATCH (u:User{id:"+rating.getUserId()+"}), (m:Movie{id:"+rating.getMovieId()+"}) CREATE (u)-[r:RATED]->(m)");
		}
		
		int timestamp = (int) ((new Date()).getTime()/1000);
		session.run("MATCH (u:User{id:"+rating.getUserId()+"})-[r:RATED]->(m:Movie{id:"+rating.getMovieId()+"})"
					+" SET r.note="+rating.getScore()+", r.timestamp="+timestamp+" return r");
		
		session.close();
		driver.close();
 	}
 	
	
	
	private List<Movie> neo4jMovieList(Integer userId) {
		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "jul31280" ) );
		Session session = driver.session();

		StatementResult result;
		if (userId == null) {
			result = session.run("MATCH (m:Movie)-[t]->(g:Genre) return m.title,m.id,g.name,g.id ORDER BY m.id");
		} else {
			result = session.run("MATCH (u:User{id:"+userId+"})-[r:RATED]->(m:Movie)-[t]->(g:Genre) return m.title,m.id,g.name,g.id ORDER BY m.id");
		}
		
		List<Movie> movies = new ArrayList<>();
		
		String previousMovieTitle = "";
		int previousMovieId = 0;
		
		List<Genre> genres = new ArrayList<>();
		
		while (result.hasNext()) {
		    Record record = result.next();
		    
		    String movieTitle = record.get("m.title").asString();
		    int movieId = record.get("m.id").asInt();
		    String genreTitle = record.get("g.name").asString();
		    int genreId = record.get("g.id").asInt();
		    
		    if (movieId != previousMovieId) {
		    	if (previousMovieId != 0) {
		    		movies.add(new Movie(previousMovieId,previousMovieTitle,genres));
		    		genres = new ArrayList<>();
		    	}
		    	
		    	previousMovieTitle = movieTitle;
		    	previousMovieId = movieId;
		    }
		    
		    genres.add(new Genre(genreId,genreTitle));
		}

		session.close();
		driver.close();
		
		return movies;
	}	
	
	private List<Rating> neo4jRatings(Integer userId) {
		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "jul31280" ) );
		Session session = driver.session();

		StatementResult result;
		result = session.run("MATCH (u:User{id:"+userId+"})-[r:RATED]->(m:Movie)-[t]->(g:Genre) return m.title,m.id,g.name,g.id,r.note ORDER BY m.id");
		
		List<Rating> ratings = new ArrayList<>();
		
		String previousMovieTitle = "";
		int previousNote = -1;
		int previousMovieId = 0;
		
		List<Genre> genres = new ArrayList<>();
		
		while (result.hasNext()) {
		    Record record = result.next();
		    
		    String movieTitle = record.get("m.title").asString();
		    int movieId = record.get("m.id").asInt();
		    String genreTitle = record.get("g.name").asString();
		    int genreId = record.get("g.id").asInt();
		    int note = record.get("r.note").asInt();
		    
		    if (movieId != previousMovieId) {
		    	if (previousMovieId != 0) {
		    		ratings.add(new Rating(new Movie(previousMovieId,previousMovieTitle,genres),userId,previousNote));
		    		genres = new ArrayList<>();
		    	}
		    	previousNote = note;
		    	previousMovieTitle = movieTitle;
		    	previousMovieId = movieId;
		    }
		    
		    genres.add(new Genre(genreId,genreTitle));
		    
		}

		ratings.add(new Rating(new Movie(previousMovieId,previousMovieTitle,genres),userId,previousNote));
		
		session.close();
		driver.close();
		
		return ratings;
	}
	
}
