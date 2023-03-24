import { StockT, QuoteT, InfoT, PredictionT } from "../types";

const handleSwipes = (direction: "left"|"right", item: StockT|QuoteT|InfoT|PredictionT) => {
    if (direction === "left") {
        switch(item.type) {
            case "Stock":
              console.log("Not interested in Stock", item.name);
                break;
            case "Quote":
              if (!item.isTrue) {
                console.log("Correct answer")
              } else {
                console.log("Wrong answer")
              }
                break;
            case "Info":
              console.log("not interested in Topic", item.name)
                break;
            case "Up or Dahn":
                if (item.direction === "Down") {
                  console.log("Correct answer")
                } else {
                  console.log("Wrong answer")
                }
                break;
        }
    } else {
        switch(item.type) {
            case "Stock":
              console.log("Interested in Stock", item.name);
                break;
            case "Quote":
              if (item.isTrue) {
                console.log("Correct answer")
              } else {
                console.log("Wrong answer")
              }
                break;
            case "Info":
                console.log("has read the article for", item.name)
                break;
            case "Up or Dahn":
                if (item.direction === "Up") {
                  console.log("Correct answer")
                } else {
                  console.log("Wrong answer")
                }
                  break;
        }
    }
}

export {handleSwipes}