package com.mr;

import java.io.*;
import java.util.*;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;

public class Coba {
	
	public static void main(String[] args){
		// Create a data source from the CSV file
	      
		try {
			File userPreferencesFile = new File(args[0]);
		    DataModel dataModel;
		      
		    dataModel = new FileDataModel(userPreferencesFile);
		    
			UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(dataModel);
		    UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(2, userSimilarity, dataModel);
		 
		    // Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
		    Recommender genericRecommender =  new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);
		 
		    
		    // Recommend 5 items for each user
		    
		    for (LongPrimitiveIterator iterator = dataModel.getUserIDs(); iterator.hasNext();)
		    {
		          long userId = iterator.nextLong();
		 
		          // Generate a list of 5 recommendations for the user
		          List<RecommendedItem> itemRecommendations = genericRecommender.recommend(userId, 5);
		 
		          
		          /*
		          System.out.format("User Id: %d%n", userId);
		 
		          if (itemRecommendations.isEmpty())
		          {
		              System.out.println("No recommendations for this user.");
		          }
		          else
		          {
		              // Display the list of recommendations
		              for (RecommendedItem recommendedItem : itemRecommendations)
		              {
		                  System.out.format("Recommened Item Id %d. Strength of the preference: %f%n", recommendedItem.getItemID(), recommendedItem.getValue());
		              }
		          }
		          */
		    }
		} catch (IOException e) {
			System.out.println("file not found");
			e.printStackTrace();
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      
	      
	  }

}
