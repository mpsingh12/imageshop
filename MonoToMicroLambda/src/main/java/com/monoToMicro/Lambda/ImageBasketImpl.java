/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.monoToMicro.Lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.utils.StringUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class ImageBasketImpl implements RequestHandler<ImageBasket, String> {
  private static final String IMAGE_TABLE_NAME = "unishop";
  private static final DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
    .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
    .httpClientBuilder(AwsCrtAsyncHttpClient.builder().maxConcurrency(50))
    .region(Region.of(System.getenv("AWS_REGION")))
    .build();
  private static final DynamoDbEnhancedAsyncClient client = DynamoDbEnhancedAsyncClient.builder()
    .dynamoDbClient(ddb)
    .build();

  static {
    try {
      final DynamoDbAsyncTable<ImageBasket> imageBasketTable = client.table(
        IMAGE_TABLE_NAME, TableSchema.fromBean(ImageBasket.class));

      imageBasketTable.describeTable().get();
    } catch (DynamoDbException | ExecutionException | InterruptedException e) {
      System.out.println(e.getMessage());
    }
  }

  @Override
  public String handleRequest(ImageBasket imageBasket, Context context) {
    return "Image Lives Matter";
  }

  public String addImageToBasket(ImageBasket imageBasket, Context context)
    throws ExecutionException, InterruptedException {
    final DynamoDbAsyncTable<ImageBasket> imageBasketTable = client.table(
      IMAGE_TABLE_NAME, TableSchema.fromBean(ImageBasket.class));

    //Get current basket
    ImageBasket currentBasket = imageBasketTable.getItem(r ->
      r.key(Key.builder().partitionValue(imageBasket.getUuid()).build())).get();

    //if there is no current basket then use the incoming basket as the new basket
    if (currentBasket == null) {
      if (imageBasket.getUuid() != null && imageBasket.getImages() != null) {
        imageBasketTable.putItem(imageBasket);
        return "Added Image to basket";
      }
      return "No basket exist and none was created";
    }

    //basket already exist, will check if item exist and add if not found
    List<Image> currentImages = currentBasket.getImages();
    List<Image> imagesToAdd = ImageBasket.getImages();

    //Assuming only one will be added but checking for null or empty values
    if (imagesToAdd != null && !imagesToAdd.isEmpty()) {
      Image imageToAdd = imagesToAdd.get(0);
      String imageToAddUuid = imageToAdd.getUuid();

      for (Image currentImage : currentImages) {
        if (currentImage.getUuid().equals(imageToAddUuid)) {
          //The image already exists, no need to add him.
          return "Image already exists!";
        }
      }

      //Image was not found, need to add and save
      currentImages.add(imageToAdd);
      currentBasket.setImages(currentImages);
      imageBasketTable.putItem(currentBasket);
      return "Added Image to basket";
    }
    return "Are you sure you added a Image?";
  }

  public String removeImageFromBasket(ImageBasket imageBasket, Context context)
    throws ExecutionException, InterruptedException {
    final DynamoDbAsyncTable<ImageBasket> imageBasketTable = client.table(
      IMAGE_TABLE_NAME, TableSchema.fromBean(ImageBasket.class));

    //Get current basket
    ImageBasket currentBasket = imageBasketTable.getItem(r ->
      r.key(Key.builder().partitionValue(imageBasket.getUuid()).build())).get();

    //if no basket exist then return an error
    if (currentBasket == null) {
      return "No basket exist, nothing to delete";
    }

    //basket exist, will check if item exist and will remove
    List<Image> currentImages = currentBasket.getImages();
    List<Image> ImagesToRemove = imageBasket.getImages();

    //Assuming only one will be removed but checking for null or empty values
    if (imagesToRemove != null && !imagesToRemove.isEmpty()) {
      Image imageToRemove = imagesToRemove.get(0);
      String imageToRemoveUuid = imageToRemove.getUuid();

      for (Image currentImage : currentImages) {
        if (currentImage.getUuid().equals(imageToRemoveUuid)) {
          currentImages.remove(currentImage);
          if (currentImages.isEmpty()) {
            //no more images in basket, will delete the basket
            imageBasketTable.deleteItem(currentBasket);
            return "Image was removed and basket was deleted!";
          } else {
            //keeping basket alive as more images are in it
            currentBasket.setImages(currentImages);
            imageBasketTable.putItem(currentBasket);
            return "Image was removed! Other images are still in basket";
          }
        }
      }

      if (currentBasket.getImages() != null && currentBasket.getImages().isEmpty()) {
        //no image to remove, will try to remove the basket nonetheless
        imageBasketTable.deleteItem(currentBasket);
      }
      return "Didn't find a image to remove";
    }
    return "Are you sure you asked to remove a Image?";
  }

  public ImageBasket getImagesBasket(ImageBasket imageBasket, Context context)
    throws ExecutionException, InterruptedException {
    final DynamoDbAsyncTable<ImageBasket> imageBasketTable = client.table(
      IMAGE_TABLE_NAME, TableSchema.fromBean(ImageBasket.class));

    if (!StringUtils.isEmpty(imageBasket.getUuid())) {
      return imageBasketTable.getItem(r ->
        r.key(Key.builder().partitionValue(imageBasket.getUuid()).build())).get();
    }

    return null;
  }
}