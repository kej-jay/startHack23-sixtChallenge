import React, { useState, useEffect } from "react";
import { Text, View, Image, Dimensions, TouchableOpacity, Modal, Pressable } from "react-native";
import Icon from "./Icon";
import { CardItemT } from "../types";
import styles, {
  DISLIKE_ACTIONS,
  WHITE,
  ICON_GREEN,
  ICON_YELLOW,
  ICON_RED
} from "../assets/styles";
import { handleSwipes } from "../functionality/swiping";

const CardItem = ({
  item,
  hasVariant
}: CardItemT) => {
  // Custom styling
  const fullWidth = Dimensions.get("window").width;

  const imageStyle = [
    {
      borderRadius: 8,
      width: fullWidth - 80,
      height: 350,
      margin: 20,
    },
  ];

  const modalImageStyle = [
    {
      borderRadius: 8,
      width: fullWidth - 80,
      height: 280,
      objectFit: "fill"
    },
  ];

  const quoteStyle = [
    {
      borderRadius: 8,
      width: fullWidth - 80,
      height: 350,
      margin: 20,
      fontStyle: "italic", 
      fontSize: 35,
    }
  ];

  const infoStyle = [
    {
      borderRadius: 8,
      width: fullWidth - 80,
      height: 350,
      margin: 20,
      fontSize: 30,
    }
  ];

  const nameStyle = [
    {
      paddingTop: 15,
      paddingBottom: 7,
      color: "#363636",
      fontSize: 30,
    },
  ];

  const typeStyle = [
    {
      paddingTop: 15,
      paddingBottom: 7,
      color: "#363636",
      fontSize: 15,
    },
  ];

  let numberToColorMap = {
    1: ICON_GREEN,
    2: ICON_YELLOW,
    3: ICON_RED
  }

  let industryToIconMap = {
    "Biotechnology": "beaker-outline",
    "Technology": "code-slash",
    "Finance": "cash-outline",
    "Energy and Transportation": "car-outline",
    "Retail and Hospitality": "business-outline",
    "Manufacturing and Healthcare": "construct-outline",
  }

  const [modalVisible, setModalVisible] = useState(false);

  return (
    <View style={styles.containerCardItem}>
      <Modal
          animationType="slide"
          transparent={true}
          visible={modalVisible}
          onRequestClose={() => {
          }}>
        <View style={styles.modalView}>
            {!item.explanation && (<Text style={nameStyle}>{item.name}</Text>)}
            {!item.explanation && (<Text>{item.description}</Text>)}

            {item.explanation && (<Text style={nameStyle}>Explanation</Text>)}
            {item.explanation && (<Text>{item.explanation}</Text>)}

            {item.type === "Stock" && (<Image source={{uri: `data:image/jpeg;base64,${item.graph2}`}} style={modalImageStyle} resizeMode="contain"/>)}
            {item.type === "Stock" && (<Image source={{uri: `data:image/jpeg;base64,${item.graph3}`}} style={modalImageStyle} resizeMode="contain"/>)}

            <View style={styles.actionsCardItem}>
              <TouchableOpacity style={styles.button} onPress={() => setModalVisible(false)}>
                <Icon name="close" color={DISLIKE_ACTIONS} size={10} />
              </TouchableOpacity>
            </View>
        </View>
      </Modal>

      {/* TYPE */}
      <Text style={typeStyle}>{item.type}</Text>

      {item.type === "Stock" && (<Pressable onPress={() => setModalVisible(true)} style={{alignItems: "center"}}>

        {/* IMAGE */}
        <Image source={{uri: `data:image/jpeg;base64,${item.image}`}} style={imageStyle} resizeMode="contain"/>

        {/* MATCHES */}
        {item.match && (
          <View style={styles.matchesCardItem}>
            <Text style={styles.matchesTextCardItem}>
              <Icon name="heart" color={WHITE} size={13} /> {item.match}% Match!
            </Text>
          </View>
        )}

        {/* NAME */}
        <Text style={nameStyle}>{item.name}</Text>
      </Pressable>)}

      {item.type === "Quote" && (
        <Pressable onPress={() => setModalVisible(true)} style={{alignItems: "center"}}>

          {/* QUOTE */}
          <Text style={quoteStyle}>{item.quote}</Text>

          {/* NAME */}
          <Text style={nameStyle}>by {item.name}</Text>
        </Pressable>
      )}

      {item.type === "Info" && (
        <Pressable onPress={() => setModalVisible(true)} style={{alignItems: "center"}}>
          {/* NAME */}
          <Text style={{...nameStyle, fontSize: 20, fontStyle:"normal"}}>{item.name}</Text>

          {/* DESCRIPTION */}
          {item.description && (
            <Text style={infoStyle}>{item.description}</Text>
          )}
        </Pressable>
      )}

      {item.type === "Up or Dahn" && (
        <Pressable onPress={() => setModalVisible(true)} style={{alignItems: "center"}}>

          {/* IMAGE */}
          <Image source={item.image} style={imageStyle}/>

          {/* NAME */}
          <Text style={nameStyle}>{item.name}</Text>
        </Pressable>
      )}
      
      {/* DESCRIPTION */}
      {item.description && item.type !== "Info" && (
        <Text style={styles.descriptionCardItem}>{item.description}</Text>
      )}

      {/* ACTIONS */}
      <View style={styles.actionsCardItem}>
        {item.type === "Stock" && (
          <>
            <TouchableOpacity style={{...styles.button, borderColor: numberToColorMap[item.trend]}} onPress={() => console.log("far Left")}>
              <Icon name="analytics" color={DISLIKE_ACTIONS} size={30} />
              <Text style={{fontSize: 7}}>Trend</Text>
            </TouchableOpacity>

            <TouchableOpacity style={{...styles.button, borderColor: numberToColorMap[item.risk]}} onPress={() => console.log("mid Left")}>
              <Icon name="bandage-outline" color={DISLIKE_ACTIONS} size={30} />
              <Text style={{fontSize: 7}}>Risk</Text>
            </TouchableOpacity>

            <TouchableOpacity style={{...styles.button, borderWidth:1}} onPress={() => console.log("mid Right")}>
              <Icon name={industryToIconMap[item.sector]} color={DISLIKE_ACTIONS} size={30} />
              <Text style={{fontSize: 7}}>Industry</Text>
            </TouchableOpacity>

            <TouchableOpacity style={{...styles.button, borderColor: numberToColorMap[item.popularity]}} onPress={() => console.log("far Right")}>
              <Icon name="rocket-outline" color={DISLIKE_ACTIONS} size={30} />
              <Text style={{fontSize: 7}}>Popularity</Text>
            </TouchableOpacity>
          </>
        )}

        {item.type === "Quote" && (
          <>
            <TouchableOpacity style={styles.noButton} onPress={() => handleSwipes("left", item)}>
              <Icon name="close" color={DISLIKE_ACTIONS} size={30} />
            </TouchableOpacity>

            <TouchableOpacity style={styles.yesButton} onPress={() => handleSwipes("right", item)}>
              <Icon name="checkmark" color={DISLIKE_ACTIONS} size={30} />
            </TouchableOpacity>
          </>
        )}

        {item.type === "Info" && (
          <>
            <TouchableOpacity style={styles.noButton} onPress={() => handleSwipes("left", item)}>
              <Icon name="close" color={DISLIKE_ACTIONS} size={30} />
            </TouchableOpacity>
          </>
        )}

        {item.type === "Up or Dahn" && (
          <>
            <TouchableOpacity style={styles.noButton} onPress={() => handleSwipes("left", item)}>
              <Icon name="arrow-down" color={DISLIKE_ACTIONS} size={30} />
            </TouchableOpacity>

            <TouchableOpacity style={styles.yesButton} onPress={() => handleSwipes("right", item)}>
              <Icon name="arrow-up" color={DISLIKE_ACTIONS} size={30} />
            </TouchableOpacity>
          </>
        )}

      </View>
    </View>
  );
};

export default CardItem;
