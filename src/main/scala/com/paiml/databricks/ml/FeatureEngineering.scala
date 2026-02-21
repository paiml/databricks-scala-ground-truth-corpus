package com.paiml.databricks.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature._

/** Feature engineering patterns for Spark MLlib pipelines.
  *
  * Provides composable feature transformers for common preprocessing tasks
  * including encoding, scaling, binning, and text feature extraction.
  */
object FeatureEngineering {

  /** One-hot encodes a categorical string column via StringIndexer + OneHotEncoder. */
  def oneHotEncode(
      df: DataFrame,
      inputCol: String,
      outputCol: String
  ): (DataFrame, StringIndexerModel, OneHotEncoderModel) = {
    val indexer = new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(s"${inputCol}_idx")
      .setHandleInvalid("keep")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol(s"${inputCol}_idx")
      .setOutputCol(outputCol)
      .setDropLast(true)
      .fit(indexed)
    val encoded = encoder.transform(indexed)

    (encoded, indexer, encoder)
  }

  /** Scales numeric features to [0, 1] using MinMaxScaler. */
  def minMaxScale(
      df: DataFrame,
      inputCol: String,
      outputCol: String
  ): (DataFrame, MinMaxScalerModel) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array(inputCol))
      .setOutputCol(s"${inputCol}_vec")
    val assembled = assembler.transform(df)

    val scaler = new MinMaxScaler()
      .setInputCol(s"${inputCol}_vec")
      .setOutputCol(outputCol)
      .fit(assembled)
    val scaled = scaler.transform(assembled)

    (scaled.drop(s"${inputCol}_vec"), scaler)
  }

  /** Standardizes features to zero mean and unit variance. */
  def standardScale(
      df: DataFrame,
      inputCol: String,
      outputCol: String
  ): (DataFrame, StandardScalerModel) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array(inputCol))
      .setOutputCol(s"${inputCol}_vec")
    val assembled = assembler.transform(df)

    val scaler = new StandardScaler()
      .setInputCol(s"${inputCol}_vec")
      .setOutputCol(outputCol)
      .setWithMean(true)
      .setWithStd(true)
      .fit(assembled)
    val scaled = scaler.transform(assembled)

    (scaled.drop(s"${inputCol}_vec"), scaler)
  }

  /** Bucketizes a continuous column into discrete bins. */
  def bucketize(
      df: DataFrame,
      inputCol: String,
      outputCol: String,
      splits: Array[Double]
  ): DataFrame = {
    val bucketizer = new Bucketizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setSplits(splits)
    bucketizer.transform(df)
  }

  /** Assembles multiple numeric columns into a single feature vector. */
  def assembleFeatures(
      df: DataFrame,
      inputCols: Array[String],
      outputCol: String = "features"
  ): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(outputCol)
      .setHandleInvalid("skip")
    assembler.transform(df)
  }

  /** Extracts TF-IDF features from a text column. */
  def tfidfFeatures(
      df: DataFrame,
      inputCol: String,
      outputCol: String,
      numFeatures: Int = 1000
  ): DataFrame = {
    val tokenizer = new Tokenizer()
      .setInputCol(inputCol)
      .setOutputCol(s"${inputCol}_tokens")
    val tokenized = tokenizer.transform(df)

    val hashingTF = new HashingTF()
      .setInputCol(s"${inputCol}_tokens")
      .setOutputCol(s"${inputCol}_tf")
      .setNumFeatures(numFeatures)
    val tfDf = hashingTF.transform(tokenized)

    val idf = new IDF()
      .setInputCol(s"${inputCol}_tf")
      .setOutputCol(outputCol)
      .fit(tfDf)
    idf.transform(tfDf)
      .drop(s"${inputCol}_tokens", s"${inputCol}_tf")
  }

  /** Imputes missing numeric values with the column mean. */
  def imputeMean(df: DataFrame, columns: Seq[String]): DataFrame = {
    val imputer = new Imputer()
      .setInputCols(columns.toArray)
      .setOutputCols(columns.map(c => s"${c}_imputed").toArray)
      .setStrategy("mean")
      .fit(df)
    imputer.transform(df)
  }
}
