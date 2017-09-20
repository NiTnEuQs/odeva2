package parser2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DB2 {
	Connection co;
	PreparedStatement ps;

	public DB2() {
		try {
			Class.forName("org.postgresql.Driver");
			co = DriverManager.getConnection("jdbc:postgresql://localhost/postgres", "postgres", "lehavre76");
			System.out.println("connexion ok");
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		} catch (SQLException e) {
			System.out.println(e);
		}

	}

	
	public void insert(Film film) {
		//francais
		//String req = "INSERT INTO movieFinderFR (idtmdb, title, titleor, genres, annee, note, nbnote, director, actors, directorUnaccented, actorsUnaccented, synopsis, poster, backdrop, duree, budget, revenu, popularity, originalLanguage, trailer, trailerName, idimdb) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String req = "INSERT INTO movieFinderEN (idtmdb, title, titleor, genres, annee, note, nbnote, director, actors, directorUnaccented, actorsUnaccented, synopsis, poster, backdrop, duree, budget, revenu, popularity, originalLanguage, trailer, trailerName, idimdb) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		//English
		//String req = "INSERT INTO filmCompletEnglish (idtmdb, title, titleor, genres, annee, note, nbnote, director, actors, synopsis, poster, duree, budget, revenu, popularity, countries, trailer, idimdb) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			ps = co.prepareStatement(req);
			
			ps.setInt(1, film.id);
			ps.setString(2, film.title);
			ps.setString(3, film.original_title);
			
			String genres = buildGenresFromList(film.genres);
			ps.setString(4, genres);
			
			ps.setString(5, film.release_date.substring(0,4));
			
			ps.setDouble(6, film.vote_average);
			ps.setInt(7, film.vote_count);
			
			String directors = getDirectorsFromCrewList(film.casts);
			ps.setString(8, directors);
			
			String actors = getActorsFromCastList(film.casts);
			ps.setString(9, actors);

            //unaccented
            String directorsUnaccented = JSONParserV3.removeAccents(directors);
            ps.setString(10, directorsUnaccented);

            String actorsUnaccented = JSONParserV3.removeAccents(actors);
            ps.setString(11, actorsUnaccented);


			ps.setString(12, film.overview);
			
			ps.setString(13, film.poster_path);
			ps.setString(14, film.backdrop_path);


			ps.setInt(15, film.runtime);
			ps.setString(16, film.budget);
			ps.setString(17, film.revenue);
			ps.setDouble(18, film.popularity);
            ps.setString(19, film.original_language);

			
			String trailerYoutube = getTrailerFromList(film.trailers);
			ps.setString(20, trailerYoutube);
			ps.setString(21, film.trailerName);
			
			ps.setString(22, film.imdb_id);

			ps.executeUpdate();
			//System.out.println("INSERT OK " + film.id);
		} catch (PSQLException e) {
			//System.out.println("déjà missss");
			if (e.getSQLState().equals("23505")) { //contrainte d'unicité
				//le film existe déjà, on l'update
				this.update(film);
			}

		} catch (SQLException e) {
			System.out.println(e);
		}
	}



	public void update(Film film) {
		//francais
		//String req = "INSERT INTO movieFinderFR (idtmdb, title, titleor, genres, annee, note, nbnote, director, actors, directorUnaccented, actorsUnaccented, synopsis, poster, backdrop, duree, budget, revenu, popularity, originalLanguage, trailer, idimdb) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String req = "UPDATE moviefinderfr " +
				"SET note=?, nbnote=?, synopsis=?, duree=?, trailer=?, trailername=? " +
				"WHERE idtmdb=?";
		//English
		//String req = "INSERT INTO filmCompletEnglish (idtmdb, title, titleor, genres, annee, note, nbnote, director, actors, synopsis, poster, duree, budget, revenu, popularity, countries, trailer, idimdb) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			ps = co.prepareStatement(req);

			ps.setDouble(1, film.vote_average);
			ps.setInt(2, film.vote_count);
			ps.setString(3, film.overview);
			ps.setInt(4, film.runtime);

			String trailerYoutube = getTrailerFromList(film.trailers);
			ps.setString(5, trailerYoutube);

			ps.setString(6, film.trailerName);
			ps.setInt(7, film.id);

			ps.executeUpdate();
			System.out.println("update OK " + film.id);
		}
		catch (PSQLException e) {
			System.out.println(e);
		} catch (SQLException e) {
			System.out.println(e);
		}
	}












/*	public void insert(String titleFR) {
		String req = "INSERT INTO films3 (titlefr) VALUES(?)";
		try {
			ps = co.prepareStatement(req);

			ps.setString(1, titleFR);

			ps.executeUpdate();
			System.out.println("INSERT OK");
		} catch (SQLException e) {
			System.out.println(e);
		}
	}
	*/
	
	public String buildGenresFromList(List<Genre> list) {
		String genres = "";
		for (Genre genre : list) {
			if (list.size() == 1) {
				genres = genre.name;
			} else {
				if (list.get(list.size()-1) != genre)  //si ce n'est pas le dernier
					genres += genre.name + ", ";
				else {
					genres += genre.name;
				}	
			}
		}
		return genres;
	}
	
	
	public String getDirectorsFromCrewList(Casting casts) {
		String directors = "";
		for (Director dir : casts.crew) {
			if (dir.job.equals("Director")) {
				if (directors.equals("")) { //premier
					directors = dir.name;
				} else {
					directors += ", " + dir.name;
				}	
			}
		}
		return directors;
	}


	public String getActorsFromCastList(Casting casts) {
		String actors = "";
		for (Acteur act : casts.cast) {
			if (actors.equals("")) { //premier
				actors = act.name;
			} else {
				actors += ", " + act.name;
			}	
		}
		return actors;
	}


	public String buildCountriesFromList(List<Country> production_countries) {
		String countries = "";
		for (Country country : production_countries) {
			if (countries.equals("")) {
				countries = country.name;
			} else {
				countries += ", " + country.name;
			}
		}
		return countries;
	}
	
	
	public String getTrailerFromList(Trailer trailers) {
		String trailer = "";
		if (trailers.youtube.size() > 0)
			trailer = trailers.youtube.get(0).source;
		
		return trailer;
	}
	


}