// import component
import React, { Component } from 'react';
import { View } from 'react-native';
import MultiSelect from 'react-native-multiple-select';

const items = [{
    id: '92iijs7yta',
    name: 'Biotechnology'
  }, {
    id: 'a0s0a8ssbsd',
    name: 'Technology'
  }, {
    id: '16hbajsabsd',
    name: 'Finance'
  }, {
    id: 'nahs75a5sj',
    name: 'Energy and Transportation'
  }, {
    id: 'nahs75a5sk',
    name: 'Retail and Hospitality'
  }, {
    id: 'nahs75a5sl',
    name: 'Manufacturing and Healthcare'
  }
];

class DropDown extends Component {

  state = {
    selectedItems : []
  };

  
  onSelectedItemsChange = selectedItems => {
    this.setState({ selectedItems });
  };

  render() {
    const { selectedItems } = this.state;

    return (
      <View style={{ flex: 1 }}>
        <MultiSelect
          hideTags
          items={items}
          uniqueKey="id"
          ref={(component) => { this.multiSelect = component }}
          onSelectedItemsChange={this.onSelectedItemsChange}
          selectedItems={selectedItems}
          selectText="Pick Items"
          searchInputPlaceholderText="Search Items..."
          onChangeInput={ (text)=> console.log(text)}
          tagRemoveIconColor="#CCC"
          tagBorderColor="#CCC"
          tagTextColor="#CCC"
          selectedItemTextColor="#CCC"
          selectedItemIconColor="#CCC"
          itemTextColor="#000"
          displayKey="name"
          searchInputStyle={{ color: '#CCC' }}
          submitButtonColor="#CCC"
          submitButtonText="Choose"
        />
        <View>
          {this.multiSelect ? this.multiSelect.getSelectedItemsExt(selectedItems) : null}
        </View>
      </View>
    );
  }
}

export default DropDown;