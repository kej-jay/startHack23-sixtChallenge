import { StockT, QuoteT, InfoT, PredictionT } from "../../types";
import IMAGE_01 from "../images/01.jpg";
import IMAGE_04 from "../images/04.jpg";

const data: (StockT|QuoteT|InfoT|PredictionT)[] = [
  {
    id: 1,
    type: "Stock",
    name: "Gamestop (GME)",
    match: 420.96,
    description:
      "Retailer of video games, consoles, and accessories, with a wide selection of new and used products.",
    message:
      "I will go back to Gotham and I will fight men Iike this but I will not become an executioner.",
    image: IMAGE_01,
  },
  {
    id: 2,
    type: "Quote",
    name: "Dave Portnoy",
    quote:
      "Stocks only go up.",
    description: "Is this quote true or not?",
    isTrue: false,
    explanation: "Stocks do not always go up because the stock market is not a guaranteed investment. The stock market reflects the underlying economic and financial conditions of a company, and these conditions are constantly changing. Companies can be doing well one day and not so well the next. Additionally, stock prices can be affected by external factors, such as geopolitical events, natural disasters and changes in the overall economy. With so many factors at play, it is impossible to predict how stocks will perform in the future. As a result, stocks do not always go up, and the potential for losses is always present."
  },
  {
    id: 3,
    type: "Info",
    name: "Short Selling",
    description:
      "Selling stocks you don't own, with the goal of buying them back later at a lower price.",
      explanation:
      "Short selling stocks is an investment strategy where an investor borrows shares from a broker and sells them on the open market, hoping to buy them back later at a lower price. The investor profits from the difference between the price at which they sold the stock and the price at which they bought it back. The investor hopes that the price of the stock will fall so they can buy the stock back at a lower price and return the stock to the broker, pocketing the difference. Short selling is a risky strategy and should be used only by investors with a high risk tolerance and a thorough understanding of the stock market.",
  },
  {
    id: 4,
    type: "Up or Dahn",
    name: "Credit Suisse",
    description:
      "Excessive risk taking, inadequate capital, and poor governance led to ...",
    direction: "Down",
    image: IMAGE_04,
    explanation: "Credit Suisse’s downfall was due to a combination of poor business decisions and a highly leveraged balance sheet. Poor decisions included aggressive investments in subprime mortgage-backed securities, exposure to risky derivatives, and a lack of risk management. This led to a highly leveraged balance sheet, leaving the bank with little capital to absorb losses. In 2008, Credit Suisse reported a $2.8 billion loss and was forced to secure a $7.7 billion bailout from the Swiss government. In addition, the bank had to pay billions of dollars in fines and settlements due to its involvement in the rigging of foreign exchange markets and other financial crimes."
  },
];

export default data;
