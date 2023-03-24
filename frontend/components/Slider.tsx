import React, { useState } from "react";
import { StyleSheet, Text, View } from "react-native";
import MultiSlider from "@ptomasroos/react-native-multi-slider";

const TIME = {  min: 1,  max: 3 }
const SliderPad = 3;

const SingleSlider = ({}) => {
  const { min, max } = TIME;
  const [width, setWidth] = useState(280);
  const [selected, setSelected] = useState(null);

  if (!selected) {
    setSelected([min]);
  }

  // Callbacks
  const onLayout = (event) => {
    setWidth(event.nativeEvent.layout.width - SliderPad * 2);
  };
  const onValuesChangeFinish = (values) => {
    setSelected(values);
    console.log('test', values)
  };

  return (
    <View onLayout={onLayout} style={styles.wrapper}>
        <MultiSlider
          min={min}
          max={max}
          step={1}
          allowOverlap
          values={selected}
          sliderLength={width}
          onValuesChangeFinish={onValuesChangeFinish}
          enableLabel={true}
          trackStyle={{
            height: 10,
            borderRadius: 8,
          }}
          markerOffsetY={3}
          selectedStyle={{
            backgroundColor: "#895CDF",
          }}
          unselectedStyle={{
            backgroundColor: "#EEF3F7",
          }}
        />
    </View>
  );
}

export default function App() {
  return (
    <View style={{ flex: 1 }}>
      <SingleSlider />
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#fff",
    alignItems: "center",
    justifyContent: "center",
  },
  wrapper: {
    flex: 1,
    margin: SliderPad * 2,
    
    justifyContent: "center",
    alignItems: "center",
  },
});
