import React, { useState, useEffect } from "react";
import { View, ImageBackground, Text, Modal, TouchableOpacity } from "react-native";
import CardStack, { Card } from "react-native-card-stack-swiper";
import { City, Filters, CardItem } from "../components";
import styles from "../assets/styles";
import DEMO from "../assets/data/demo";
import { handleSwipes } from "../functionality/swiping";
import { StockT, QuoteT, InfoT, PredictionT } from "../types";

const Home = () => {
  const [swiper, setSwiper] = useState<CardStack | null>(null);
  const [cards, setCards] = useState<StockT|QuoteT|InfoT[]>([]);

  const getData = async () => {
      const url1 = 'http://127.0.0.1:5000/stock'
      const url2 = 'http://127.0.0.1:5000/quotes'
      const url3 = 'http://127.0.0.1:5000/stock_info'

      console.log("request out")
      const response1 = await fetch(url1)
      const data1 = await response1.json()
      let stockData:StockT[] = data1;
      stockData.forEach(element => {
        element.image = element["graph1"]
      });

      const response2 = await fetch(url2)
      const data2 = await response2.json()

      const response3 = await fetch(url3)
      const data3 = await response3.json()

      console.log("mixing card stack");
      const combinedArray = [...data1, ...data2, ...data3]
      const shuffledArray = combinedArray.sort((a, b) => 0.5 - Math.random())
      return shuffledArray
  }

  useEffect(() => {
    getData().then((res) => setCards(res))
  }, []);

  return (
    <ImageBackground
      source={require("../assets/images/bg.png")}
      style={styles.bg}
    >
      <View style={styles.containerHome}>
        <View style={styles.top}>
          <City />
          <Filters />
        </View>

        <CardStack
          loop
          verticalSwipe={false}
          renderNoMoreCards={() => null}
          ref={(newSwiper): void => setSwiper(newSwiper)}
        >
          {cards.map((item) => (                
            <Card key={item.id}
            onSwipedLeft={() => handleSwipes("left", item)}
            onSwipedRight={() => handleSwipes("right", item)}
            >
              <CardItem
                item={item}
              />
            </Card>
          ))}
        </CardStack>
      </View>
    </ImageBackground>
  );
};

export default Home;
